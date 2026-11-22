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
var __values = (this && this.__values) || function(o) {
    var s = typeof Symbol === "function" && Symbol.iterator, m = s && o[s], i = 0;
    if (m) return m.call(o);
    if (o && typeof o.length === "number") return {
        next: function () {
            if (o && i >= o.length) o = void 0;
            return { value: o && o[i++], done: !o };
        }
    };
    throw new TypeError(s ? "Object is not iterable." : "Symbol.iterator is not defined.");
};
var __read = (this && this.__read) || function (o, n) {
    var m = typeof Symbol === "function" && o[Symbol.iterator];
    if (!m) return o;
    var i = m.call(o), r, ar = [], e;
    try {
        while ((n === void 0 || n-- > 0) && !(r = i.next()).done) ar.push(r.value);
    }
    catch (error) { e = { error: error }; }
    finally {
        try {
            if (r && !r.done && (m = i["return"])) m.call(i);
        }
        finally { if (e) throw e.error; }
    }
    return ar;
};
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.bookmarkManager = bookmarkManager;
/**
 * Interface for the piece of software responsible for keeping track of current active bookmarks accross the driver.
 * @interface
 * @since 5.0
 */
var BookmarkManager = /** @class */ (function () {
    /**
     * @constructor
     * @private
     */
    function BookmarkManager() {
        throw new Error('Not implemented');
    }
    /**
     * Method called when the bookmarks get updated when a transaction finished.
     *
     * This method will be called when auto-commit queries finish and when explicit transactions
     * get committed.
     *
     * @param {Iterable<string>} previousBookmarks The bookmarks used when starting the transaction
     * @param {Iterable<string>} newBookmarks The new bookmarks received at the end of the transaction.
     * @returns {void}
    */
    BookmarkManager.prototype.updateBookmarks = function (previousBookmarks, newBookmarks) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                throw new Error('Not implemented');
            });
        });
    };
    /**
     * Method called by the driver to get the bookmarks.
     *
     * @returns {Iterable<string>} The set of bookmarks
     */
    BookmarkManager.prototype.getBookmarks = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                throw new Error('Not implemented');
            });
        });
    };
    return BookmarkManager;
}());
exports.default = BookmarkManager;
/**
 * @typedef {Object} BookmarkManagerConfig
 *
 * @since 5.0
 * @property {Iterable<string>} [initialBookmarks] Defines the initial set of bookmarks. The key is the database name and the values are the bookmarks.
 * @property {function():Promise<Iterable<string>>} [bookmarksSupplier] Called for supplying extra bookmarks to the BookmarkManager
 * @property {function(bookmarks: Iterable<string>): Promise<void>} [bookmarksConsumer] Called when the set of bookmarks  get updated
 */
/**
 * Provides an configured {@link BookmarkManager} instance.
 *
 * @since 5.0
 * @param {BookmarkManagerConfig} [config={}]
 * @returns {BookmarkManager}
 */
function bookmarkManager(config) {
    if (config === void 0) { config = {}; }
    var initialBookmarks = new Set(config.initialBookmarks);
    return new Neo4jBookmarkManager(initialBookmarks, config.bookmarksSupplier, config.bookmarksConsumer);
}
var Neo4jBookmarkManager = /** @class */ (function () {
    function Neo4jBookmarkManager(_bookmarks, _bookmarksSupplier, _bookmarksConsumer) {
        this._bookmarks = _bookmarks;
        this._bookmarksSupplier = _bookmarksSupplier;
        this._bookmarksConsumer = _bookmarksConsumer;
    }
    Neo4jBookmarkManager.prototype.updateBookmarks = function (previousBookmarks, newBookmarks) {
        return __awaiter(this, void 0, void 0, function () {
            var bookmarks, previousBookmarks_1, previousBookmarks_1_1, bm, newBookmarks_1, newBookmarks_1_1, bm;
            var e_1, _a, e_2, _b;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        bookmarks = this._bookmarks;
                        try {
                            for (previousBookmarks_1 = __values(previousBookmarks), previousBookmarks_1_1 = previousBookmarks_1.next(); !previousBookmarks_1_1.done; previousBookmarks_1_1 = previousBookmarks_1.next()) {
                                bm = previousBookmarks_1_1.value;
                                bookmarks.delete(bm);
                            }
                        }
                        catch (e_1_1) { e_1 = { error: e_1_1 }; }
                        finally {
                            try {
                                if (previousBookmarks_1_1 && !previousBookmarks_1_1.done && (_a = previousBookmarks_1.return)) _a.call(previousBookmarks_1);
                            }
                            finally { if (e_1) throw e_1.error; }
                        }
                        try {
                            for (newBookmarks_1 = __values(newBookmarks), newBookmarks_1_1 = newBookmarks_1.next(); !newBookmarks_1_1.done; newBookmarks_1_1 = newBookmarks_1.next()) {
                                bm = newBookmarks_1_1.value;
                                bookmarks.add(bm);
                            }
                        }
                        catch (e_2_1) { e_2 = { error: e_2_1 }; }
                        finally {
                            try {
                                if (newBookmarks_1_1 && !newBookmarks_1_1.done && (_b = newBookmarks_1.return)) _b.call(newBookmarks_1);
                            }
                            finally { if (e_2) throw e_2.error; }
                        }
                        if (!(typeof this._bookmarksConsumer === 'function')) return [3 /*break*/, 2];
                        return [4 /*yield*/, this._bookmarksConsumer(__spreadArray([], __read(bookmarks), false))];
                    case 1:
                        _c.sent();
                        _c.label = 2;
                    case 2: return [2 /*return*/];
                }
            });
        });
    };
    Neo4jBookmarkManager.prototype.getBookmarks = function () {
        return __awaiter(this, void 0, void 0, function () {
            var bookmarks, suppliedBookmarks, suppliedBookmarks_1, suppliedBookmarks_1_1, bm;
            var e_3, _a;
            var _b;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        bookmarks = new Set(this._bookmarks);
                        if (!(typeof this._bookmarksSupplier === 'function')) return [3 /*break*/, 2];
                        return [4 /*yield*/, this._bookmarksSupplier()];
                    case 1:
                        suppliedBookmarks = (_b = _c.sent()) !== null && _b !== void 0 ? _b : [];
                        try {
                            for (suppliedBookmarks_1 = __values(suppliedBookmarks), suppliedBookmarks_1_1 = suppliedBookmarks_1.next(); !suppliedBookmarks_1_1.done; suppliedBookmarks_1_1 = suppliedBookmarks_1.next()) {
                                bm = suppliedBookmarks_1_1.value;
                                bookmarks.add(bm);
                            }
                        }
                        catch (e_3_1) { e_3 = { error: e_3_1 }; }
                        finally {
                            try {
                                if (suppliedBookmarks_1_1 && !suppliedBookmarks_1_1.done && (_a = suppliedBookmarks_1.return)) _a.call(suppliedBookmarks_1);
                            }
                            finally { if (e_3) throw e_3.error; }
                        }
                        _c.label = 2;
                    case 2: return [2 /*return*/, __spreadArray([], __read(bookmarks), false)];
                }
            });
        });
    };
    return Neo4jBookmarkManager;
}());
