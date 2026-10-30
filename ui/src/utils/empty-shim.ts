// Empty stub for Node built-ins (net, tls, fs, ...) that the
// neo4j-driver-bolt-connection node-channel transitively imports.
// neo4jBrowserChannelPlugin redirects the channel itself to the
// browser variant, but Rolldown still resolves the node channel's
// dependency graph during analysis. Pointing those built-ins at this
// module short-circuits the resolution to a harmless empty namespace.
//
// Anything reachable via `import x from 'net'` etc. evaluates to an
// empty object. Real consumers go through the browser channel which
// uses the WebSocket global instead and never touches these.
export default {};
