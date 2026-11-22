"use strict";
/**
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var pool_config_1 = __importDefault(require("./pool-config"));
var error_1 = require("../../error");
var logger_1 = require("../logger");
var Pool = /** @class */ (function () {
    /**
     * @param {function(acquisitionContext: object, address: ServerAddress, function(address: ServerAddress, resource: object): Promise<object>): Promise<object>} create
     *                an allocation function that creates a promise with a new resource. It's given an address for which to
     *                allocate the connection and a function that will return the resource to the pool if invoked, which is
     *                meant to be called on .dispose or .close or whatever mechanism the resource uses to finalize.
     * @param {function(acquisitionContext: object, resource: object): boolean} validateOnAcquire
     *                called at various times when an instance is acquired
     *                If this returns false, the resource will be evicted
     * @param {function(resource: object): boolean} validateOnRelease
     *                called at various times when an instance is released
     *                If this returns false, the resource will be evicted
     * @param {function(resource: object): Promise<void>} destroy
     *                called with the resource when it is evicted from this pool
     * @param {function(resource: object, observer: { onError }): void} installIdleObserver
     *                called when the resource is released back to pool
     * @param {function(resource: object): void} removeIdleObserver
     *                called when the resource is acquired from the pool
     * @param {PoolConfig} config configuration for the new driver.
     * @param {Logger} log the driver logger.
     */
    function Pool(_a) {
        var _b = _a.create, create = _b === void 0 ? function (acquisitionContext, address, release) { return __awaiter(_this, void 0, void 0, function () { return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, Promise.reject(new Error('Not implemented'))];
                case 1: return [2 /*return*/, _a.sent()];
            }
        }); }); } : _b, _c = _a.destroy, destroy = _c === void 0 ? function (conn) { return __awaiter(_this, void 0, void 0, function () { return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, Promise.resolve()];
                case 1: return [2 /*return*/, _a.sent()];
            }
        }); }); } : _c, _d = _a.validateOnAcquire, validateOnAcquire = _d === void 0 ? function (acquisitionContext, conn) { return true; } : _d, _e = _a.validateOnRelease, validateOnRelease = _e === void 0 ? function (conn) { return true; } : _e, _f = _a.installIdleObserver, installIdleObserver = _f === void 0 ? function (conn, observer) { } : _f, _g = _a.removeIdleObserver, removeIdleObserver = _g === void 0 ? function (conn) { } : _g, _h = _a.config, config = _h === void 0 ? pool_config_1.default.defaultConfig() : _h, _j = _a.log, log = _j === void 0 ? logger_1.Logger.noOp() : _j;
        var _this = this;
        this._create = create;
        this._destroy = destroy;
        this._validateOnAcquire = validateOnAcquire;
        this._validateOnRelease = validateOnRelease;
        this._installIdleObserver = installIdleObserver;
        this._removeIdleObserver = removeIdleObserver;
        this._maxSize = config.maxSize;
        this._acquisitionTimeout = config.acquisitionTimeout;
        this._pools = {};
        this._pendingCreates = {};
        this._acquireRequests = {};
        this._activeResourceCounts = {};
        this._release = this._release.bind(this);
        this._log = log;
        this._closed = false;
    }
    /**
     * Acquire and idle resource fom the pool or create a new one.
     * @param {object} acquisitionContext the acquisition context used for create and validateOnAcquire connection
     * @param {ServerAddress} address the address for which we're acquiring.
     * @param {object} config the config
     * @param {boolean} config.requireNew Indicate it requires a new resource
     * @return {Promise<Object>} resource that is ready to use.
     */
    Pool.prototype.acquire = function (acquisitionContext, address, config) {
        return __awaiter(this, void 0, void 0, function () {
            var key, allRequests, requests, elapsedTime;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        key = address.asKey();
                        allRequests = this._acquireRequests;
                        requests = allRequests[key];
                        elapsedTime = (acquisitionContext.startTime != null && acquisitionContext.startTime !== 0) ? new Date().getTime() - acquisitionContext.startTime : 0;
                        if (elapsedTime >= this._acquisitionTimeout) {
                            throw this.getAcquisitionTimeoutError(address);
                        }
                        if (requests == null) {
                            allRequests[key] = [];
                        }
                        return [4 /*yield*/, new Promise(function (resolve, reject) {
                                var timeoutId = setTimeout(function () {
                                    // acquisition timeout fired
                                    // remove request from the queue of pending requests, if it's still there
                                    // request might've been taken out by the release operation
                                    var pendingRequests = allRequests[key];
                                    if (pendingRequests != null) {
                                        allRequests[key] = pendingRequests.filter(function (item) { return item !== request; });
                                    }
                                    if (request.isCompleted()) {
                                        // request already resolved/rejected by the release operation; nothing to do
                                    }
                                    else {
                                        // request is still pending and needs to be failed
                                        request.reject(_this.getAcquisitionTimeoutError(address));
                                    }
                                }, _this._acquisitionTimeout - elapsedTime);
                                if (typeof timeoutId === 'object') {
                                    // eslint-disable-next-line
                                    // @ts-ignore
                                    timeoutId.unref();
                                }
                                var request = new PendingRequest(key, acquisitionContext, config, resolve, reject, timeoutId, _this._log);
                                allRequests[key].push(request);
                                _this._processPendingAcquireRequests(address);
                            })];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    Pool.prototype.getAcquisitionTimeoutError = function (address) {
        var key = address.asKey();
        var activeCount = this.activeResourceCount(address);
        var idleCount = this.has(address) ? this._pools[key].length : 0;
        return (0, error_1.newError)("Connection acquisition timed out in ".concat(this._acquisitionTimeout, " ms. Pool status: Active conn count = ").concat(activeCount, ", Idle conn count = ").concat(idleCount, "."));
    };
    /**
     * Destroy all idle resources for the given address.
     * @param {ServerAddress} address the address of the server to purge its pool.
     * @returns {Promise<void>} A promise that is resolved when the resources are purged
     */
    Pool.prototype.purge = function (address) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._purgeKey(address.asKey())];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    Pool.prototype.apply = function (address, resourceConsumer) {
        var key = address.asKey();
        if (key in this._pools) {
            this._pools[key].apply(resourceConsumer);
        }
    };
    /**
     * Destroy all idle resources in this pool.
     * @returns {Promise<void>} A promise that is resolved when the resources are purged
     */
    Pool.prototype.close = function () {
        return __awaiter(this, void 0, void 0, function () {
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        this._closed = true;
                        return [4 /*yield*/, Promise.all(Object.keys(this._pools).map(function (key) { return __awaiter(_this, void 0, void 0, function () { return __generator(this, function (_a) {
                                switch (_a.label) {
                                    case 0: return [4 /*yield*/, this._purgeKey(key)];
                                    case 1: return [2 /*return*/, _a.sent()];
                                }
                            }); }); })).then()];
                    case 1: 
                    /**
                     * The lack of Promise consuming was making the driver do not close properly in the scenario
                     * captured at result.test.js:it('should handle missing onCompleted'). The test was timing out
                     * because while waiting for the driver close.
                     *
                     * Consuming the Promise.all or by calling then or by awaiting in the result inside this method solved
                     * the issue somehow.
                     *
                     * PS: the return of this method was already awaited at PooledConnectionProvider.close, but the await bellow
                     * seems to be need also.
                     */
                    return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /**
     * Keep the idle resources for the provided addresses and purge the rest.
     * @returns {Promise<void>} A promise that is resolved when the other resources are purged
     */
    Pool.prototype.keepAll = function (addresses) {
        return __awaiter(this, void 0, void 0, function () {
            var keysToKeep, keysPresent, keysToPurge;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        keysToKeep = addresses.map(function (a) { return a.asKey(); });
                        keysPresent = Object.keys(this._pools);
                        keysToPurge = keysPresent.filter(function (k) { return !keysToKeep.includes(k); });
                        return [4 /*yield*/, Promise.all(keysToPurge.map(function (key) { return __awaiter(_this, void 0, void 0, function () { return __generator(this, function (_a) {
                                switch (_a.label) {
                                    case 0: return [4 /*yield*/, this._purgeKey(key)];
                                    case 1: return [2 /*return*/, _a.sent()];
                                }
                            }); }); })).then()];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /**
     * Check if this pool contains resources for the given address.
     * @param {ServerAddress} address the address of the server to check.
     * @return {boolean} `true` when pool contains entries for the given key, <code>false</code> otherwise.
     */
    Pool.prototype.has = function (address) {
        return address.asKey() in this._pools;
    };
    /**
     * Get count of active (checked out of the pool) resources for the given key.
     * @param {ServerAddress} address the address of the server to check.
     * @return {number} count of resources acquired by clients.
     */
    Pool.prototype.activeResourceCount = function (address) {
        var _a;
        return (_a = this._activeResourceCounts[address.asKey()]) !== null && _a !== void 0 ? _a : 0;
    };
    Pool.prototype._getOrInitializePoolFor = function (key) {
        var pool = this._pools[key];
        if (pool == null) {
            pool = new SingleAddressPool();
            this._pools[key] = pool;
            this._pendingCreates[key] = 0;
        }
        return pool;
    };
    Pool.prototype._acquire = function (acquisitionContext, address, requireNew) {
        return __awaiter(this, void 0, void 0, function () {
            var key, pool, resource_1, valid, e_1, numConnections, resource, numConnections, resource_2;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (this._closed) {
                            throw (0, error_1.newError)('Pool is closed, it is no more able to serve requests.');
                        }
                        key = address.asKey();
                        pool = this._getOrInitializePoolFor(key);
                        if (!!requireNew) return [3 /*break*/, 10];
                        _a.label = 1;
                    case 1:
                        if (!(pool.length > 0)) return [3 /*break*/, 10];
                        resource_1 = pool.pop();
                        if (resource_1 == null) {
                            return [3 /*break*/, 1];
                        }
                        resourceAcquired(key, this._activeResourceCounts);
                        if (this._removeIdleObserver != null) {
                            this._removeIdleObserver(resource_1);
                        }
                        valid = false;
                        _a.label = 2;
                    case 2:
                        _a.trys.push([2, 4, , 6]);
                        return [4 /*yield*/, this._validateOnAcquire(acquisitionContext, resource_1)];
                    case 3:
                        valid = _a.sent();
                        return [3 /*break*/, 6];
                    case 4:
                        e_1 = _a.sent();
                        resourceReleased(key, this._activeResourceCounts);
                        pool.removeInUse(resource_1);
                        return [4 /*yield*/, this._destroy(resource_1)];
                    case 5:
                        _a.sent();
                        throw e_1;
                    case 6:
                        if (!valid) return [3 /*break*/, 7];
                        // idle resource is valid and can be acquired
                        if (this._log.isDebugEnabled()) {
                            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
                            this._log.debug("".concat(resource_1, " acquired from the pool ").concat(key));
                        }
                        return [2 /*return*/, { resource: resource_1, pool: pool }];
                    case 7:
                        resourceReleased(key, this._activeResourceCounts);
                        pool.removeInUse(resource_1);
                        return [4 /*yield*/, this._destroy(resource_1)];
                    case 8:
                        _a.sent();
                        _a.label = 9;
                    case 9: return [3 /*break*/, 1];
                    case 10:
                        // Ensure requested max pool size
                        if (this._maxSize > 0) {
                            numConnections = this.activeResourceCount(address) + this._pendingCreates[key];
                            if (numConnections >= this._maxSize) {
                                // Will put this request in queue instead since the pool is full
                                return [2 /*return*/, { resource: null, pool: pool }];
                            }
                        }
                        // there exist no idle valid resources, create a new one for acquisition
                        // Keep track of how many pending creates there are to avoid making too many connections.
                        this._pendingCreates[key] = this._pendingCreates[key] + 1;
                        _a.label = 11;
                    case 11:
                        _a.trys.push([11, , 15, 16]);
                        numConnections = this.activeResourceCount(address) + pool.length;
                        if (!(numConnections >= this._maxSize && requireNew)) return [3 /*break*/, 13];
                        resource_2 = pool.pop();
                        if (!(resource_2 != null)) return [3 /*break*/, 13];
                        if (this._removeIdleObserver != null) {
                            this._removeIdleObserver(resource_2);
                        }
                        pool.removeInUse(resource_2);
                        return [4 /*yield*/, this._destroy(resource_2)];
                    case 12:
                        _a.sent();
                        _a.label = 13;
                    case 13: return [4 /*yield*/, this._create(acquisitionContext, address, function (address, resource) { return __awaiter(_this, void 0, void 0, function () { return __generator(this, function (_a) {
                            switch (_a.label) {
                                case 0: return [4 /*yield*/, this._release(address, resource, pool)];
                                case 1: return [2 /*return*/, _a.sent()];
                            }
                        }); }); })];
                    case 14:
                        // Invoke callback that creates actual connection
                        resource = _a.sent();
                        pool.pushInUse(resource);
                        resourceAcquired(key, this._activeResourceCounts);
                        if (this._log.isDebugEnabled()) {
                            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
                            this._log.debug("".concat(resource, " created for the pool ").concat(key));
                        }
                        return [3 /*break*/, 16];
                    case 15:
                        this._pendingCreates[key] = this._pendingCreates[key] - 1;
                        return [7 /*endfinally*/];
                    case 16: return [2 /*return*/, { resource: resource, pool: pool }];
                }
            });
        });
    };
    Pool.prototype._release = function (address, resource, pool) {
        return __awaiter(this, void 0, void 0, function () {
            var key;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        key = address.asKey();
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, , 9, 10]);
                        if (!pool.isActive()) return [3 /*break*/, 6];
                        return [4 /*yield*/, this._validateOnRelease(resource)];
                    case 2:
                        if (!!(_a.sent())) return [3 /*break*/, 4];
                        if (this._log.isDebugEnabled()) {
                            this._log.debug(
                            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
                            "".concat(resource, " destroyed and can't be released to the pool ").concat(key, " because it is not functional"));
                        }
                        pool.removeInUse(resource);
                        return [4 /*yield*/, this._destroy(resource)];
                    case 3:
                        _a.sent();
                        return [3 /*break*/, 5];
                    case 4:
                        if (this._installIdleObserver != null) {
                            this._installIdleObserver(resource, {
                                onError: function (error) {
                                    _this._log.debug(
                                    // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
                                    "Idle connection ".concat(resource, " destroyed because of error: ").concat(error));
                                    var pool = _this._pools[key];
                                    if (pool != null) {
                                        _this._pools[key] = pool.filter(function (r) { return r !== resource; });
                                        pool.removeInUse(resource);
                                    }
                                    // let's not care about background clean-ups due to errors but just trigger the destroy
                                    // process for the resource, we especially catch any errors and ignore them to avoid
                                    // unhandled promise rejection warnings
                                    _this._destroy(resource).catch(function () { });
                                }
                            });
                        }
                        pool.push(resource);
                        if (this._log.isDebugEnabled()) {
                            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
                            this._log.debug("".concat(resource, " released to the pool ").concat(key));
                        }
                        _a.label = 5;
                    case 5: return [3 /*break*/, 8];
                    case 6:
                        // key has been purged, don't put it back, just destroy the resource
                        if (this._log.isDebugEnabled()) {
                            this._log.debug(
                            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
                            "".concat(resource, " destroyed and can't be released to the pool ").concat(key, " because pool has been purged"));
                        }
                        pool.removeInUse(resource);
                        return [4 /*yield*/, this._destroy(resource)];
                    case 7:
                        _a.sent();
                        _a.label = 8;
                    case 8: return [3 /*break*/, 10];
                    case 9:
                        resourceReleased(key, this._activeResourceCounts);
                        this._processPendingAcquireRequests(address);
                        return [7 /*endfinally*/];
                    case 10: return [2 /*return*/];
                }
            });
        });
    };
    Pool.prototype._purgeKey = function (key) {
        return __awaiter(this, void 0, void 0, function () {
            var pool, destructionList, resource;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        pool = this._pools[key];
                        destructionList = [];
                        if (!(pool != null)) return [3 /*break*/, 2];
                        while (pool.length > 0) {
                            resource = pool.pop();
                            if (resource == null) {
                                continue;
                            }
                            if (this._removeIdleObserver != null) {
                                this._removeIdleObserver(resource);
                            }
                            destructionList.push(this._destroy(resource));
                        }
                        pool.close();
                        // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                        delete this._pools[key];
                        return [4 /*yield*/, Promise.all(destructionList)];
                    case 1:
                        _a.sent();
                        _a.label = 2;
                    case 2: return [2 /*return*/];
                }
            });
        });
    };
    Pool.prototype._processPendingAcquireRequests = function (address) {
        var _this = this;
        var key = address.asKey();
        var requests = this._acquireRequests[key];
        if (requests != null) {
            var pendingRequest_1 = requests.shift(); // pop a pending acquire request
            if (pendingRequest_1 != null) {
                this._acquire(pendingRequest_1.context, address, pendingRequest_1.requireNew)
                    .catch(function (error) {
                    // failed to acquire/create a new connection to resolve the pending acquire request
                    // propagate the error by failing the pending request
                    pendingRequest_1.reject(error);
                    return { resource: null, pool: null };
                })
                    .then(function (_a) {
                    var resource = _a.resource, pool = _a.pool;
                    // there is not situation where the pool resource is not null and the
                    // pool is null.
                    if (resource != null && pool != null) {
                        // managed to acquire a valid resource from the pool
                        if (pendingRequest_1.isCompleted()) {
                            // request has been completed, most likely failed by a timeout
                            // return the acquired resource back to the pool
                            _this._release(address, resource, pool)
                                .catch(function (error) {
                                if (_this._log.isDebugEnabled()) {
                                    // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
                                    _this._log.debug("".concat(resource, " could not be release back to the pool. Cause: ").concat(error));
                                }
                            });
                        }
                        else {
                            // request is still pending and can be resolved with the newly acquired resource
                            pendingRequest_1.resolve(resource); // resolve the pending request with the acquired resource
                        }
                    }
                    else {
                        // failed to acquire a valid resource from the pool
                        // return the pending request back to the pool
                        if (!pendingRequest_1.isCompleted()) {
                            if (_this._acquireRequests[key] == null) {
                                _this._acquireRequests[key] = [];
                            }
                            _this._acquireRequests[key].unshift(pendingRequest_1);
                        }
                    }
                }).catch(function (error) { return pendingRequest_1.reject(error); });
            }
            else {
                // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                delete this._acquireRequests[key];
            }
        }
    };
    return Pool;
}());
/**
 * Increment active (checked out of the pool) resource counter.
 * @param {string} key the resource group identifier (server address for connections).
 * @param {Object.<string, number>} activeResourceCounts the object holding active counts per key.
 */
function resourceAcquired(key, activeResourceCounts) {
    var _a;
    var currentCount = (_a = activeResourceCounts[key]) !== null && _a !== void 0 ? _a : 0;
    activeResourceCounts[key] = currentCount + 1;
}
/**
 * Decrement active (checked out of the pool) resource counter.
 * @param {string} key the resource group identifier (server address for connections).
 * @param {Object.<string, number>} activeResourceCounts the object holding active counts per key.
 */
function resourceReleased(key, activeResourceCounts) {
    var _a;
    var currentCount = (_a = activeResourceCounts[key]) !== null && _a !== void 0 ? _a : 0;
    var nextCount = currentCount - 1;
    if (nextCount > 0) {
        activeResourceCounts[key] = nextCount;
    }
    else {
        // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
        delete activeResourceCounts[key];
    }
}
var PendingRequest = /** @class */ (function () {
    function PendingRequest(key, context, config, resolve, reject, timeoutId, log) {
        this._key = key;
        this._context = context;
        this._resolve = resolve;
        this._reject = reject;
        this._timeoutId = timeoutId;
        this._log = log;
        this._completed = false;
        this._config = config !== null && config !== void 0 ? config : {};
    }
    Object.defineProperty(PendingRequest.prototype, "context", {
        get: function () {
            return this._context;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(PendingRequest.prototype, "requireNew", {
        get: function () {
            var _a;
            return (_a = this._config.requireNew) !== null && _a !== void 0 ? _a : false;
        },
        enumerable: false,
        configurable: true
    });
    PendingRequest.prototype.isCompleted = function () {
        return this._completed;
    };
    PendingRequest.prototype.resolve = function (resource) {
        if (this._completed) {
            return;
        }
        this._completed = true;
        clearTimeout(this._timeoutId);
        if (this._log.isDebugEnabled()) {
            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
            this._log.debug("".concat(resource, " acquired from the pool ").concat(this._key));
        }
        this._resolve(resource);
    };
    PendingRequest.prototype.reject = function (error) {
        if (this._completed) {
            return;
        }
        this._completed = true;
        clearTimeout(this._timeoutId);
        this._reject(error);
    };
    return PendingRequest;
}());
var SingleAddressPool = /** @class */ (function () {
    function SingleAddressPool() {
        this._active = true;
        this._elements = [];
        this._elementsInUse = new Set();
    }
    SingleAddressPool.prototype.isActive = function () {
        return this._active;
    };
    SingleAddressPool.prototype.close = function () {
        this._active = false;
        this._elements = [];
        this._elementsInUse = new Set();
    };
    SingleAddressPool.prototype.filter = function (predicate) {
        this._elements = this._elements.filter(predicate);
        return this;
    };
    SingleAddressPool.prototype.apply = function (resourceConsumer) {
        this._elements.forEach(resourceConsumer);
        this._elementsInUse.forEach(resourceConsumer);
    };
    Object.defineProperty(SingleAddressPool.prototype, "length", {
        get: function () {
            return this._elements.length;
        },
        enumerable: false,
        configurable: true
    });
    SingleAddressPool.prototype.pop = function () {
        var element = this._elements.pop();
        if (element != null) {
            this._elementsInUse.add(element);
        }
        return element;
    };
    SingleAddressPool.prototype.push = function (element) {
        this._elementsInUse.delete(element);
        return this._elements.push(element);
    };
    SingleAddressPool.prototype.pushInUse = function (element) {
        this._elementsInUse.add(element);
    };
    SingleAddressPool.prototype.removeInUse = function (element) {
        this._elementsInUse.delete(element);
    };
    return SingleAddressPool;
}());
exports.default = Pool;
