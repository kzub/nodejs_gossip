var async = require('async');

log = function(L, msg){
		var args = [];
		for(var k in arguments){
			args.push(arguments[k]);
		}

		var msgs = args.slice(1);
		msgs.unshift('[' + L + ']');
		console.log.apply(console, msgs);
	}

// log('W', { d : { d: { d: { d: { a : 1, b : function(){} }}}}, k : function(){} })
log('I', '0');
async.seq(
	function(cb){
		log('I', '1');
		cb();
	},
	function(cb){
		log('I', '2');
		cb();
	},
	function(cb){
		log('I', '3');
		cb(null, 'ok');
	}
)(function(err, data){
	log('I', 'fin', err, data);
});

a=new Buffer(10).toString('hex');
console.log(a)