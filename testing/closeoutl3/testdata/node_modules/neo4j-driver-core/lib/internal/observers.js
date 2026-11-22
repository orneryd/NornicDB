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
Object.defineProperty(exports, "__esModule", { value: true });
exports.FailedObserver = exports.CompletedObserver = void 0;
var CompletedObserver = /** @class */ (function () {
    function CompletedObserver() {
    }
    CompletedObserver.prototype.subscribe = function (observer) {
        apply(observer, observer.onKeys, []);
        apply(observer, observer.onCompleted, {});
    };
    CompletedObserver.prototype.cancel = function () {
        // do nothing
    };
    CompletedObserver.prototype.pause = function () {
        // do nothing
    };
    CompletedObserver.prototype.resume = function () {
        // do nothing
    };
    CompletedObserver.prototype.prepareToHandleSingleResponse = function () {
        // do nothing
    };
    CompletedObserver.prototype.markCompleted = function () {
        // do nothing
    };
    CompletedObserver.prototype.onError = function (error) {
        // nothing to do, already finished
        // eslint-disable-next-line
        // @ts-ignore: not available in ES oldest supported version
        throw new Error('CompletedObserver not supposed to call onError', { cause: error });
    };
    return CompletedObserver;
}());
exports.CompletedObserver = CompletedObserver;
var FailedObserver = /** @class */ (function () {
    function FailedObserver(_a) {
        var error = _a.error, onError = _a.onError;
        this._error = error;
        this._beforeError = onError;
        this._observers = [];
        this.onError(error);
    }
    FailedObserver.prototype.subscribe = function (observer) {
        apply(observer, observer.onError, this._error);
        this._observers.push(observer);
    };
    FailedObserver.prototype.onError = function (error) {
        apply(this, this._beforeError, error);
        this._observers.forEach(function (o) { return apply(o, o.onError, error); });
    };
    FailedObserver.prototype.cancel = function () {
        // do nothing
    };
    FailedObserver.prototype.pause = function () {
        // do nothing
    };
    FailedObserver.prototype.resume = function () {
        // do nothing
    };
    FailedObserver.prototype.markCompleted = function () {
        // do nothing
    };
    FailedObserver.prototype.prepareToHandleSingleResponse = function () {
        // do nothing
    };
    return FailedObserver;
}());
exports.FailedObserver = FailedObserver;
function apply(thisArg, func, param) {
    if (func != null) {
        func.bind(thisArg)(param);
    }
}
