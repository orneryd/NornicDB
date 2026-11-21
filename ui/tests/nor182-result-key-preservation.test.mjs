// NOR-182: Cypher result conversion must keep every own enumerable map key
// on both result transports.
//
// The module under test is the real ui/src/utils/api.ts, loaded through the
// project's own Vite pipeline (vite.config.ts) so the production transform
// (import.meta.env, extensionless relative imports) and the installed
// neo4j-driver are exercised. Bolt values are decoded by the driver's own
// Packstream decoder; HTTP values are parsed from the /tx/commit JSON body by
// the client itself. No conversion logic is copied into this file.
import {after, before, test} from "node:test";
import assert from "node:assert/strict";
import {createRequire} from "node:module";
import {fileURLToPath} from "node:url";
import {createServer} from "vite";

const uiRoot = fileURLToPath(new URL("..", import.meta.url));
const require = createRequire(import.meta.url);
const {Unpacker} = require("neo4j-driver-bolt-connection/lib/packstream/packstream-v1");
const {alloc} = require("neo4j-driver-bolt-connection/lib/channel/channel-buf");

// Fixtures are written as JSON text so `__proto__` is an ordinary own property
// instead of a prototype assignment performed by this file.
const SPECIAL_KEYS_JSON =
  '{"__proto__":7,"constructor":10,"":11," ":12,"ordinary":13,' +
  '"nested":{"__proto__":8," ":9},"list":[{"__proto__":14}]}';
const OBJECT_VALUED_PROTO_JSON = '{"__proto__":{"polluted":1},"ordinary":2}';
const LARGE_INTEGER_JSON = '{"__proto__":7,"big":9007199254740992}';

let server;
let api;
let realFetch;

before(async () => {
  server = await createServer({
    root: uiRoot,
    configFile: `${uiRoot}/vite.config.ts`,
    appType: "custom",
    logLevel: "error",
    server: {middlewareMode: true},
  });
  // resolveBoltURL() reads window.location; discovery is pre-seeded per test
  // so no network call is made.
  globalThis.window = {location: {protocol: "http:", hostname: "localhost"}};
  realFetch = globalThis.fetch;
  ({api} = await server.ssrLoadModule("/src/utils/api.ts"));
});

after(async () => {
  globalThis.fetch = realFetch;
  await server?.close();
});

// resetClient returns the shared client to a known transport state with cached
// discovery, so executeCypher() takes the requested path directly.
function resetClient({boltAvailable = true} = {}) {
  api.defaultDatabase = "nornic";
  api.discovery = {
    bolt_enabled: boltAvailable,
    transaction: "",
    neo4j_version: "",
    neo4j_edition: "",
  };
  api.boltUnavailable = !boltAvailable;
  api.boltDriver = null;
  api.boltDriverPromise = null;
}

// stubBoltRecord impersonates the driver session/record surface that
// runCypherOverBolt consumes. The value itself is a real driver-decoded value.
function stubBoltRecord(value) {
  api.boltDriver = {
    session() {
      return {
        async run() {
          return {
            records: [
              {
                keys: ["value"],
                get: (key) => (key === "value" ? value : undefined),
              },
            ],
          };
        },
        async close() {},
      };
    },
  };
}

async function runBoltPath(value) {
  resetClient();
  stubBoltRecord(value);
  const response = await api.executeCypher("RETURN value");
  return firstRow(response, "Bolt");
}

async function runHttpPath(fixtureJson) {
  resetClient({boltAvailable: false});
  globalThis.fetch = async () =>
    new Response(txCommitBody(fixtureJson), {
      status: 200,
      headers: {"Content-Type": "application/json"},
    });
  const response = await api.executeCypher("RETURN value");
  return firstRow(response, "HTTP");
}

function firstRow(response, transport) {
  assert.equal(
    response.errors,
    undefined,
    `${transport} response must carry no errors`,
  );
  assert.equal(
    response.results.length,
    1,
    `${transport} response must contain one result`,
  );
  assert.deepEqual(response.results[0].columns, ["value"]);
  assert.equal(response.results[0].data.length, 1);
  return response.results[0].data[0].row[0];
}

// txCommitBody builds the /tx/commit payload the server sends when asked for
// resultDataContents ["row","graph"]: the bare property map in `row` plus one
// `meta` entry per leaf value.
function txCommitBody(fixtureJson) {
  const meta = new Array(leafCount(JSON.parse(fixtureJson)))
    .fill("null")
    .join(",");
  return (
    '{"results":[{"columns":["value"],"data":[{"row":[' +
    fixtureJson +
    '],"meta":[' +
    meta +
    ']}]}],"errors":[]}'
  );
}

function leafCount(value) {
  if (Array.isArray(value)) {
    return value.reduce((total, entry) => total + leafCount(entry), 0);
  }
  if (value !== null && typeof value === "object") {
    return Object.values(value).reduce(
      (total, entry) => total + leafCount(entry),
      0,
    );
  }
  return 1;
}

// packValue encodes the fixture as Packstream so the value reaches api.ts from
// the installed driver's decoder. Only the marker forms used by the fixtures
// are implemented.
function packValue(value) {
  const bytes = [];
  const write = (entry) => {
    if (typeof entry === "number") {
      assert.ok(Number.isInteger(entry), "fixtures contain integers only");
      if (entry >= -16 && entry <= 127) {
        bytes.push(entry & 0xff);
        return;
      }
      const int64 = Buffer.alloc(9);
      int64[0] = 0xcb;
      int64.writeBigInt64BE(BigInt(entry), 1);
      bytes.push(...int64);
      return;
    }
    if (typeof entry === "string") {
      const encoded = Buffer.from(entry, "utf8");
      assert.ok(encoded.length < 16, "fixture strings fit the tiny-string form");
      bytes.push(0x80 + encoded.length, ...encoded);
      return;
    }
    if (Array.isArray(entry)) {
      assert.ok(entry.length < 16, "fixture lists fit the tiny-list form");
      bytes.push(0x90 + entry.length);
      entry.forEach(write);
      return;
    }
    const entries = Object.entries(entry);
    assert.ok(entries.length < 16, "fixture maps fit the tiny-map form");
    bytes.push(0xa0 + entries.length);
    for (const [key, nested] of entries) {
      write(key);
      write(nested);
    }
  };
  write(value);
  return Uint8Array.from(bytes);
}

// unpackBoltValue produces the value runCypherOverBolt receives from
// rec.get(...): neo4j Integer instances (the Unpacker keeps them) that the UI
// converts itself.
function unpackBoltValue(fixtureJson) {
  return new Unpacker(false).unpack(
    alloc(Buffer.from(packValue(JSON.parse(fixtureJson)))),
  );
}

function assertSpecialKeys(value, transport) {
  assert.deepEqual(
    value,
    JSON.parse(SPECIAL_KEYS_JSON),
    `${transport} conversion must keep every own enumerable key`,
  );
  assert.equal(Object.getPrototypeOf(value), Object.prototype);
  assert.equal(Object.hasOwn(value, "__proto__"), true);
  assert.equal(Object.hasOwn(value, "constructor"), true);
  assert.equal(Object.hasOwn(value, ""), true);
  assert.equal(Object.hasOwn(value, " "), true);
  assert.equal(Object.hasOwn(value.nested, "__proto__"), true);
  assert.equal(Object.hasOwn(value.nested, " "), true);
  assert.equal(Object.hasOwn(value.list[0], "__proto__"), true);
  // Ordinary conversion behaviour is untouched: nested maps stay plain objects
  // and Bolt integers become numbers.
  assert.equal(typeof value.constructor, "number");
  assert.equal(value.constructor, 10);
  assert.equal(typeof value.ordinary, "number");
  assert.equal(Object.getPrototypeOf(value.nested), Object.prototype);
  assert.equal(Object.getPrototypeOf(value.list[0]), Object.prototype);
}

function assertObjectValuedProto(value, transport) {
  assert.equal(
    Object.hasOwn(value, "__proto__"),
    true,
    `${transport} conversion must keep an object-valued __proto__ as data`,
  );
  assert.deepEqual(value["__proto__"], {polluted: 1});
  assert.equal(
    Object.getPrototypeOf(value),
    Object.prototype,
    `${transport} conversion must not swap the result prototype`,
  );
  assert.equal(value.polluted, undefined, "no inherited value may leak in");
  assert.deepEqual(Object.keys(value), ["__proto__", "ordinary"]);
  assert.equal(Object.prototype.polluted, undefined, "global prototype untouched");
}

test("Bolt results keep own __proto__, constructor, empty and whitespace keys", async () => {
  const decoded = unpackBoltValue(SPECIAL_KEYS_JSON);
  assert.equal(
    Object.hasOwn(decoded, "__proto__"),
    true,
    "installed driver must decode __proto__",
  );
  assertSpecialKeys(await runBoltPath(decoded), "Bolt");
});

test("Bolt results keep an object-valued __proto__ without swapping the prototype", async () => {
  assertObjectValuedProto(
    await runBoltPath(unpackBoltValue(OBJECT_VALUED_PROTO_JSON)),
    "Bolt",
  );
});

test("Bolt integers keep their number and safe-range String conversion", async () => {
  const row = await runBoltPath(unpackBoltValue(LARGE_INTEGER_JSON));
  assert.equal(Object.hasOwn(row, "__proto__"), true);
  assert.equal(typeof row["__proto__"], "number");
  assert.equal(row["__proto__"], 7);
  assert.equal(row.big, "9007199254740992");
});

test("HTTP /tx/commit results keep the same keys as Bolt results", async () => {
  const httpRow = await runHttpPath(SPECIAL_KEYS_JSON);
  assertSpecialKeys(httpRow, "HTTP");
  assert.deepEqual(
    httpRow,
    await runBoltPath(unpackBoltValue(SPECIAL_KEYS_JSON)),
    "both transports must produce identical rows",
  );
});

test("HTTP /tx/commit results keep an object-valued __proto__ without swapping the prototype", async () => {
  assertObjectValuedProto(await runHttpPath(OBJECT_VALUED_PROTO_JSON), "HTTP");
});
