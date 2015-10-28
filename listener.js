/**
 * Created by wing on 2015/5/26.
 */
var events = require('events'), util = require('util');
var _ = require('lodash');

var requests = [];

exports.request = function (request) {
    requests.push(request);
};

exports.response = function (message) {
    requests.forEach(function (request) {
        request.emit('response', message);
    })
};

var Request = function (id, timeout) {
    events.EventEmitter.call(this);

    var self = this;
    this.id = id;
    timeout = timeout || 3000;
    this.timer = setTimeout(function () {
        self.emit('_timeout');
    }, timeout);
    this.on('response', function (message) {
        if (message.id == self.id){
            clearTimeout(self.timer);
            _.remove(requests, function (r) {
                return r.id == self.id;
            });
            self.emit('data', message.data);
        }
    });
    this.on('_timeout', function () {
        _.remove(requests, function (r) {
            return r.id == self.id;
        });
        self.emit('timeout', '');
    })
};

util.inherits(Request, events.EventEmitter);
exports.Request = Request;