/**
 * Created by wing on 2015/5/11.
 */
var RedisClient = require('../redis');
var q = require('q');
var agentClient = new RedisClient({
    "module": 'agent',
    "host": "192.168.230.231",
    "port": 6379,
    "options": {}
});

var outletClient = new RedisClient({
    "module": 'outlet',
    "host": "192.168.230.231",
    "port": 6379,
    "options": {}
});

outletClient.on('request', function (messages) {
    //console.log(messages);
});

outletClient.on('event', function (messages) {
    //console.log(messages);
});

agentClient.on('request_test', function (request) {
    //console.log(messages);
    request.response({echo: 'i m test ' + request.data});
});

agentClient.on('event', function (event) {
    console.log('event ', event.data);
});
setTimeout(function () {
    q().then(function () {
        return outletClient.request('agent', 'test', {echo: 'i m outlet'});
    }).then(function (data) {
        console.log(data);
    });
}, 1000);
setTimeout(function () {
    outletClient.event('agent', 'test', {test: 'event'})
}, 2000);
