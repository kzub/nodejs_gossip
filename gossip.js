var Step = require('step');
var async = require('async');
var log = console.log;
var net = require('net');
var os = require('os');
var redis = require('redis')
var crypto = require('crypto');
var fs = require('fs');
var http = require('http');
var http = require("http");
var url = require("url");
var path = require("path");

log = function(){};
log.i = log.e = log.d = log;

// повторять процесс подключения несколько раз
// пока подключаещься не слушать heartbeat
// точнее не реагировать, что давно небыло нетмастера


function Gossip(cmgr, cif){
	// dont forget 'new' operand
	if(!(this instanceof Gossip)){
		return new Gossip(cmgr, cif);
	}
	var self = this;

	// constants
	var MAX_IN_PEERS = 3;
	var MAX_OUT_PEERS = 2;
	var MIN_HOPS_TO_CONNECT = 3; //4
	var MAX_KNOWN_PEERS = 100;
	var CMD_LENGTH = 4;
	var HASH_LENGTH = 32;
	var CHECK_CONN_INTERVAL_MS = 3000;
	var CHECK_HASHES_INTERVAL_MS = 60000;
	var HEARTBEAT_INTERVAL_MS = 2000;
	var NO_HEARTBEAT_TO_ELECT_NETMASTER_MS = 6000;
	var NO_TIME_UPDATE_TO_ELECT_SYSMASTER_MS = 10000;
	var MAX_PIF_LOCK_SEC = 60;
	// variables
	var peerOutConnections = [];
	var peerOutConnecting = 0;
	var peerInConnections = [];
	var msgsIhaveSeen = {};
	var knownPeers = [];
	var peersIAM;
	var peerEnabled = true;

	if(!cmgr ||
		!(cmgr.get instanceof Function) ||
		!(cmgr.add instanceof Function) ||
		!(cmgr.lock instanceof Function)){
			throw 'i need peers interface';
	}

	if(!cif ||
		!(cif.connect instanceof Function) ||
		!(cif.bind instanceof Function) ||
		!(cif.on instanceof Function)){
			throw 'i need connections interface';
	}

	var areTheSamePeers = function(p1, p2){
		if(!p1 || !p2){
			return false;
		}

		if(p1 instanceof Array){
			if(p2 instanceof Array){
				for(var i = 0; i < p1.length; i++){
					if(p2.indexOf(p1[i]) > -1){
						return true;
					}
				}
				return false;
			}
			else{
				return p1.indexOf(p2) > -1;
			}
		}
		else{
			if(p2 instanceof Array){
				return p2.indexOf(p1) > -1;
			}
			else{
				return p1 === p2;
			}
		}
	};

	var iAmThisPeer = function(peer){
		return areTheSamePeers(peersIAM, peer);
	};

	var iAmConnectedTo = function(peer){
		// 'peer' may be string or array of strings
		var result = false;
		peerForEveryConnection(function(pc){
			if(result){ return; }
			if(areTheSamePeers(pc.peer, peer)){
				result = true;
			}
		});
		return result;
	};

	var peerEndConnection = function(iface){
		if(!iface){
			error
			log.e('?!?!?!?!?! no iface');
			return;
		}

		iface.end();

		// skip clearing not accepted connections
		if(!iface.accepted){
			return;
		}

		var cleanedPeerConnections = [];
		for(var idx in peerInConnections){
			var pc = peerInConnections[idx];
			if(!areTheSamePeers(pc.peer, iface.peer)){
				cleanedPeerConnections.push(pc);
			}
		}
		peerInConnections = cleanedPeerConnections;

		cleanedPeerConnections = [];
		for(var idx in peerOutConnections){
			var pc = peerOutConnections[idx];
			if(!areTheSamePeers(pc.peer, iface.peer)){
				cleanedPeerConnections.push(pc);
			}
		}
		peerOutConnections = cleanedPeerConnections;
	};

	var getMyNetCapacity = function(){
		var data = {
			peer : peersIAM.slice(),
			lcap : (MAX_IN_PEERS - peerInConnections.length), // listen capacity
			ccap : (MAX_OUT_PEERS - peerOutConnections.length)  // out conn cap
			// cntd : ((peerInConnections.length + peerOutConnections.length) !== 0) // network connected
		};
		return data;
	};

	var getMyNetLoadMax = function(){
		return MAX_IN_PEERS + MAX_OUT_PEERS;
	};

	var getMyNetLoad = function(){
		return peerInConnections.length + peerOutConnections.length;
	};

	var peerConnectTo = function(peer, directmsg, mainCallback){
		// call it once
		var callback = function(a, b, c){
			callback = function(){};
			mainCallback(a, b, c);
		};

		if(!peerEnabled){
			return callback();
		}

		if(!peer){
			return callback('no_peer');
		}
		var iface;

		Step(
			function connect(){
				cif.connect(peer, this);
			},
			function handshake(err, conn){
				if(err){
					// log.e('connect', err);
					return callback(err);
				}
				iface = conn;
				iface.peer = peer;

				var nextStep = this;

				iface.on('error', function(iface, err){
					return callback(err, null, iface);
				});

				iface.on('data', function(iface, answer) {
					nextStep(null, answer);
				});

				iface.on('end', function() {
					log.i('disconnected by server', peer);
					return callback(null, null, iface);
				});

				iface.on('connect', function() {
					// send direct message
					if(directmsg){
						iface.send(directmsg);
						iface.end();
						return;
					}

					var data = getMyNetCapacity();
					var msg = makeMessage('hllo', data);
					iface.send(msg);
				});
			},
			function parseranswer(err, answerData){
				if(err){
					log.e('handshake', err);
					return callback(err);
				}

				if(!answerData){
					return callback(/* connect to next peer*/);
				}

				log.d('answer', answerData);
				var answer = getDataFromMessage(answerData);
				this(null, answer);
			},
			function finale(err, answer){
				if(err){
					log.e('parse', err);
				}

				callback(err, answer, iface);
			}
		);
	};

	// update list of known peers
	var peerAddNew = function(peerList, movetotop){
		if(!(peerList instanceof Array)){
			peerList = [peerList];
		}
		var newpeers = [];
		for(var idx in peerList){
			var newPeer = peerList[idx];
			var positionInList = knownPeers.indexOf(newPeer);
			if(positionInList === -1){
				// add new peers to the begining
				knownPeers.unshift(newPeer);
				newpeers.push(newPeer);
			}
			// move to top fresh peers
			else if(movetotop){
				knownPeers.splice(positionInList, 1);
				knownPeers.unshift(newPeer);
			}
		}
		// limit peers base for one host
		knownPeers = knownPeers.slice(0, MAX_KNOWN_PEERS);
		return newpeers;
	}

	// internal routine
	var peerMakeConnectionInt = function(thispeers, directmsg, callback){
		var haveFreeSlots = MAX_OUT_PEERS - peerOutConnections.length - peerOutConnecting > 0;
		if(!directmsg && !haveFreeSlots){
			log.d(peersIAM[0], 'EXIT making conn', !!directmsg, peerOutConnecting, peerOutConnections.length, MAX_OUT_PEERS);
			return callback();
		}
		if(!directmsg){
			peerOutConnecting++;
		}
		var peersConnected = 0;

		Step(
			function getpeers(){
				// make a copy
				var peerList = thispeers ? (thispeers instanceof Array ? thispeers.slice(/*make a copy*/) : [thispeers]) : knownPeers.slice(/*make a copy*/);
				// use known peers to get the connection at first
				if(!peerList.length){
					log.d('no peers to connect');
					if(!directmsg){
						peerOutConnecting--;
					}
					return callback();
				}

				log.d('making connection to:', peerList);
				var peer;
				var connectingStepOver = this;

				async.until(
					function check(){
						return peerList.length === 0;
					},
					function iter(tryNextPeer) {
						peer = peerList.pop();

						// skip connecting to myself or already connected to
						if(iAmThisPeer(peer) || iAmConnectedTo(peer)){
							return tryNextPeer();
						}

						log.d(peersIAM[0], 'conn to ', peer, directmsg || '-');

						// try to connect to selected peer
						peerConnectTo(peer, directmsg, function(err, answer, iface){
							if(err){
								peerEndConnection(iface);
								log.e('cant connect to', peer, err && err.code);
								return tryNextPeer();
							}

							// exit if not error
							if(directmsg){
								peerEndConnection(iface);
								return tryNextPeer();
							}

							if(!answer){
								peerEndConnection(iface);
								// try next peer
								return tryNextPeer();
							}

							if(answer.cmd === 'wfcn'){
								// wait in silence
								peerEndConnection(iface);
								return connectingStepOver();
							}

							// we just connected well
							if(answer.cmd === 'acpt'){
								log.d('connected to', peer);
								// data == connection interface
								iface.accepted = true;
								peersConnected++;
								iface.type = 'out';
								peerOutConnections.push(iface);

								// redirect messages
								iface.on('data', srvGotMsg);
								// handle disconnect and errors
								iface.on('end',  srvGotDisconnection);
								iface.on('error',srvGotDisconnection);

								return connectingStepOver();
							}

							log.e('EEEEEEEEEEEEEEEEEEEEEE??????????????');
							EEE;
							peerEndConnection(iface);
							tryNextPeer();
						});
					},
					function fin(err){
						if(err){
							log.e('can\'t connect to peer', err, peer);
						}
						connectingStepOver();
					}
				);
			},
			function connectionfailed(err){
				if(err){
					log.e('connectionfailed', err);
				}

				if(!directmsg){
					peerOutConnecting--;
				}
				callback(err, peersConnected);
			}
		);
	};

	// check this instance connected to any other gossip instance
	var peerMakeConnection = function(thispeers, callback){
		if(thispeers instanceof Function){
			// thispeers == callback
			return peerMakeConnectionInt(null, null, thispeers);
		}
		peerMakeConnectionInt(thispeers, null, callback || function(){});
	};

	var peerDirectMessage = function(thispeers, directmsg, callback){
		peerMakeConnectionInt(thispeers, directmsg, callback || function(){});
	};

	var iSawThisMessage = function(msg){
		// skip known messages
		if(msgsIhaveSeen[msg.hash]){
			return true;
		}

		msgsIhaveSeen[msg.hash] = Date.now()
		return false;
	};

	var checkMessageHashes = function(){
		var clearMsgsIhaveSeen = {};
		var now = Date.now();
		// remove hashes older than CHECK_HASHES_INTERVAL_MS from memory
		for(var k in msgsIhaveSeen){
			if(msgsIhaveSeen[k] + CHECK_HASHES_INTERVAL_MS < now){
				clearMsgsIhaveSeen[k] = msgsIhaveSeen[k];
			}
		}
		msgsIhaveSeen = clearMsgsIhaveSeen;
	};

	self.onMessage = function(data){
		log.i('MSG:', data)
		if(data.cmd == 'color'){
			this.color = data.color;
		}
	};

	self.message = function(data, sendtome){
		self.sysMsg('umsg', data, sendtome);
	}

	self.sysMsg = function(cmd, data, sendtome){
		if(cmd.length !== CMD_LENGTH){
			log.e("CMD_WRONG_LENGTH:", cmd);
			return;
		}
		var msg = makeMessage(cmd, data);
		sendMessage(msg);

		if(sendtome){
			var iface = {
				type : 'fake',
				peer : peersIAM[0],
				on   : function(event, fn){},
				send : function(msg){},
				end  : function(){}
			};
			srvGotMsg(iface, msg);
		}
	};

	var srvGotMsg = function(iface, msgdata){
		if(msgdata.slice(0,4) !== 'hrtb'){
			log.d('srvGot', peersIAM, msgdata);
		}
		var msg = getDataFromMessage(msgdata);

		// skip known messages
		if(iSawThisMessage(msg)){
			return;
		}

		if(msg.cmd === 'hllo'){
			// update this peer info
			iface.peer = msg.data.peer;
			// add to knowlege base
			peerAddNew(msg.data.peer);
			netOnHeartBeat();

			// accept connection if possible
			if(peerInConnections.length < MAX_IN_PEERS){
				iface.accepted = true;
				iface.type = 'in';
				peerInConnections.push(iface);
				iface.send(makeMessage('acpt'));
				msg.data.ccap--;
			}
			// close connection if not accepted
			else{
				iface.send(makeMessage('wfcn'));
				iface.end();
			}

			// if not connected to network yet tell other about new member
			if(/*msg.data.cntd === false &&*/ (msg.data.lcap > 0 || msg.data.ccap > 0)){
				var data = {
					peer : msg.data.peer,
					lcap : msg.data.lcap,
					ccap : msg.data.ccap,
					hop  : 0  // hops for not leting sequented connections
				};
				var newmsg = makeMessage('memb', data);
				// send to all except new one
				sendMessage(newmsg, iface.peer, true);
			}
			return;
		}

		// new member in network
		if(msg.cmd === 'memb'){
			netOnHeartBeat();

			// if(msg.data.hop === undefined && msg.data.total &&
			// 	(msg.data.total > (msg.data.ccap + msg.data.lcap))){
			// 		msg.data.hop = 0;
			// 	console.log('aha', (msg.data.ccap + msg.data.lcap))
			// }

			if(msg.data.hop !== undefined){
				msg.data.hop++;
			}
			// update last known peer (to let new members connect to it)
			peerAddNew(msg.data.peer, true);

			var allowConnect = function(mode){
				return true;
				// if(msg.data.hop === undefined){ return true; }
				return msg.data.hop > MIN_HOPS_TO_CONNECT;
			};
			// let peer connect to us
			if(allowConnect() && peerInConnections.length < MAX_IN_PEERS && msg.data.ccap > 0){
				// send direct message
				peerDirectMessage(msg.data.peer, makeMessage('imwt', peersIAM)); // i waiting for you
				msg.data.ccap--;
			}
			// or we connect to peer
			else if(allowConnect() && peerOutConnections.length < MAX_OUT_PEERS && msg.data.lcap > 0){
				// try to connect if it possible
				peerMakeConnection(msg.data.peer);
				msg.data.lcap--;
			}

			// route message to network
			if(msg.data.lcap > 0 || msg.data.ccap > 0){
				var newmsg = makeMessage('memb', msg.data, msg.hash);
				sendMessage(newmsg, iface.peer);
			}
			return;
		}

		// direct message
		if(msg.cmd === 'imwt'){
			var peer = msg.data;
			// peerAddNew(peer);
			if(peerOutConnections.length < MAX_OUT_PEERS){
				// try to connect if it possible
				log.d(peersIAM[0], 'mkconn to', peer);
				netOnHeartBeat();
				peerMakeConnection(peer);
			}
			// close connection
			iface.end();
			return;
		}

		// heartbeat
		if(msg.cmd === 'hrtb'){
			// var peerMaster = msg.data.netMaster;
			// peerAddNew(peerMaster)
			netOnHeartBeat(msg.data);
			// route to network
			sendMessage(msgdata, iface.peer);
			return;
		}

		if(msg.cmd === 'smel'){
				var iHavePeers = getMyNetLoad();
				var usePeers  = iHavePeers ? knownPeers.slice(0, MIN_HOPS_TO_CONNECT).reverse() : peersIAM;
				var answ = makeMessage('rjin', {
					netMaster	: peersIAM,
					rang		: peerMyRang,
					usePeers	: peersIAM
				});

				peerDirectMessage(msg.data.sender, answ);
			// close connection
			return iface.end();
		}

		// user (app) message
		if(msg.cmd === 'umsg'){
			// send to newtwork
			self.onMessage(msg.data);
			// route to network
			setTimeout(function(){
				sendMessage(msgdata, iface.peer);
			}, 200);
			return;
		}

		if(msg.cmd === 'rjin'){
			// update new netmaster
			netMasterRang = undefined;
			// tell other network about me
			netOnHeartBeat(msg.data);
			// route to network reset command
			peerForEveryConnection(
				function every(iface, cb){
					iface.send(msgdata, function(){
						// peerEndConnection(iface);
						iface.end();
						cb();
					});
				},
				function finish(err){
					if(err){
						log.e('rejoin err:', err);
					}

					var membData = getMyNetCapacity();
					delete membData.hop;
					membData.total = membData.lcap + membData.ccap;

					var joinRequest = makeMessage('memb', membData);
					var peerToJoin  = msg.data.netMaster;
					// log.d(msg.data.usePeers.toString())
					peerDirectMessage(peerToJoin, joinRequest);
					netOnHeartBeat();
				},
				'async' // need for wait to free connections slots
			);

			// close connection
			// return iface.end();
		}

		// todo
		// on reset connecting to sysmaster pool

		// disconnect
		if(msg.cmd === 'rset'){
			// reset only one node
			if(msg.data.peer){
				if(areTheSamePeers(msg.data.peer, peersIAM)){
					peerForEveryConnection(peerEndConnection);
				}
				else{
					sendMessage(msgdata, iface.peer);
				}
				return;
			}
			// reset all nodes in network
			peerForEveryConnection(
				function every(iface){
					iface.send(msgdata, function(){
						peerEndConnection(iface);
					});
					// peerEndConnection(iface); // broken disc if needed
				}
			);

			return;
		}

		log.e('unknown message', msgdata);
		iface.end();
	};

	var peerForEveryConnection = function(fn, fnFinish, mode){
		// if we want to run in syncr mode
		var conns = peerInConnections.concat(peerOutConnections);

		async.each(conns,
			function iter(pc, cb){
				fn(pc, cb);
				if(mode !== 'async'){
					setImmediate(cb);
				}
			},
			function fin(err){
				if(err){
					log.e('PFEC ERROR:', err);
				}
				if(fnFinish){
					fnFinish();
				}
			}
		);
	};

	var sendMessage = function(text, exceptPeer, handleMessageMyself){
		peerForEveryConnection(function(iface){
			if(areTheSamePeers(iface.peer, exceptPeer)){
				// log.i('skip to send', peersIAM, exceptPeer);
				return;
			}
			iface.send(text);
		});

		if(handleMessageMyself){
			// TODO rewrite to function!!!!!!!!!
			var iface = {
				type : 'fake',
				peer : peersIAM[0],
				on   : function(event, fn){},
				send : function(msg){},
				end  : function(){}
			};
			srvGotMsg(iface, text);
		}
	};

	var makeHash = function(text){
		var hash = crypto.createHash('md5')
			.update(Date.now() + (text || ''))
			.digest('hex');

		if(hash.length < HASH_LENGTH){
			var addlen = HASH_LENGTH - hash.length + 1;
			hash = hash.concat(new Array(addlen).join('x'));
		}
		else{
			hash = hash.slice(0, HASH_LENGTH);
		}

		return hash;
	};

	var makeMessage = function(cmd, data, hash){
		if(cmd.length < CMD_LENGTH){
			var addlen = CMD_LENGTH - cmd.length + 1;
			cmd = cmd.concat(new Array(addlen).join('x'));
		}
		else if(cmd.length > CMD_LENGTH){
			log.e(cmd, data, hash);
			throw 'TOO LONG CMD:' + cmd;
		}

		var datatype = 's';
		if(['string', 'boolean', 'number', 'undefined'].indexOf(typeof data) === -1){
			data = JSON.stringify(data);
			datatype = 'j';
		}
		var msghash = hash || makeHash(data);
		return cmd + msghash + datatype + (data || '');
	};

	var getDataFromMessage = function(msg){
		var cmd  = msg.slice(0, CMD_LENGTH);
		var hash = msg.slice(CMD_LENGTH, HASH_LENGTH + CMD_LENGTH);
		var datatype = msg.slice(HASH_LENGTH + CMD_LENGTH, HASH_LENGTH + CMD_LENGTH + 1);
		var data = msg.slice(HASH_LENGTH + CMD_LENGTH + 1);

		if(datatype === 'j'){
			data = JSON.parse(data);
		}

		return { cmd : cmd, hash : hash, datatype : datatype, data : data };
	};

	var srvGotDisconnection = function(iface){
		log.d('disconnected from server', iface.peer);
		peerEndConnection(iface);

		// notify others about free slot
		if(iface.accepted){
			var data = getMyNetCapacity();
			var newmsg = makeMessage('memb', data);
			sendMessage(newmsg, iface.peer);
		}
	};

	var peerUpdateHubAndConnect = function(callback){
		Step(
			function updateHost(){
				peerUpdateHub(this);
			},
			function makeNewConnections(err){
				if(err){
					log.e('updatehub', err);
				}

				peerMakeConnection(this);
			},
			function fin(err) {
				if(err){
					log.e('make conn', err);
				}

				if(callback){
					callback();
				}
			}
		);
	};

	var peerUpdateHub = function(callback){
		var peerList;
		Step(
			function lock(){
				cmgr.lock(MAX_PIF_LOCK_SEC, this); // maxlocktime
			},
			function getpeerlist(err){
				if(err){
					log.e('lock', err);
				}

				log.e('++++++++')
				// get list of ready to connect
				cmgr.get(this);
			},
			function sayiamhere(err, pl){
				if(err){
					log.e('getpeerlist', err);
				}
				peerList = pl;
				// update list
				peerAddNew(pl);
				// set me as ready to connect
				cmgr.add(peersIAM.slice(), this);
			},
			function unlock(err){
				if(err){
					log.e('sayiamhere', err);
				}
				cmgr.unlock(this);
			},
			function fisnish(err){
				if(err){
					log.e(err);
				}
				log.i('---------')
				callback(err, peerList);
			}
		);
	};

	var netMaster, netMasterRang, sysMaster;
	var netMasterBecameTime;
	var peerMyRang = Math.random();

	var netSayIAmNetMaster = function(){
		log.i('IAM NETMASTER', peersIAM);
		// save time i became a master
		netMaster			= peersIAM;
		netMasterRang		= peerMyRang;
		lastNetHeartBeat	= Date.now();

		// update who is the master
		var data = {
			netMaster	: peersIAM,
			rang		: peerMyRang
		};

		// and one for other
		var msg = makeMessage('hrtb', data);
		sendMessage(msg);
	};

	var lastNetHeartBeat = Date.now();
	var sysCheckMasterInProgress;
	var netCheckSysMaster = function(){
		if(sysCheckMasterInProgress){
			return;
		}

		var sysCheckMasterInProgress = true;
		var terminate = function(){
			sysCheckMasterInProgress = false;
		};

		Step(
			function whoTheMaster(){
				cmgr.getMaster(this);
			},
			function sysMasterElection(err, masterInfo){
				if(err){
					log.e('whoTheMaster', err);
					return terminate();
				}

				var now = Date.now();
				var myMasterInfo = [peersIAM.join('/'), now].join('|');
				var info, masterPeers, masterTime, sysMasterExist;
				// reset state
				sysMaster = null;

				// master exist
				if(masterInfo){
					info = masterInfo.split('|');
					masterPeers = info[0].split('/');
					masterTime = info[1];
					sysMasterExist = true;
				}
				// log.i('SYSMASTER INFO', masterPeers, masterTime, now);
				// INTERVAL OK
				if(sysMasterExist && ((now - masterTime) < NO_TIME_UPDATE_TO_ELECT_SYSMASTER_MS)){
					sysMaster = masterPeers;
					// if it is me update record with now time
					if(areTheSamePeers(masterPeers, peersIAM)){
						log.i('IAM SYSMASTER', peersIAM);
						cmgr.setMaster(myMasterInfo, this);
						return terminate();
					}

					// otherwise try to connect to this master
					log.i('TELL SYSMASTER ABOUT ME', peersIAM, masterPeers);
					// sysmaster election
					var data = {
						sender : peersIAM
					};
					var msg = makeMessage('smel', data);
					peerDirectMessage(masterPeers, msg);
					return terminate();
				}

				log.i('BECAME A SYSMASTER', myMasterInfo);
				// INTERVAL FAIL. became a new master
				sysMaster = peersIAM;
				cmgr.setMaster(myMasterInfo, this);
			},
			function final(err){
				if(err){
					log.e('isHeOld', err);
				}
				// now iam system master
				terminate();
			}
		);
	};

	var myHeartBeat = function(){
		var now = Date.now();
		// if i am master tell others about it
		if(areTheSamePeers(netMaster, peersIAM)){
			netSayIAmNetMaster();
			//find other masters in system
			netCheckSysMaster();
			return;
		}

		// this knowledge is only for netmaster's
		sysMaster = null;

		// if no master to long time, became one
		if(now - lastNetHeartBeat > NO_HEARTBEAT_TO_ELECT_NETMASTER_MS){
			// and try to became a master
			netSayIAmNetMaster();
		}
	};

	var netOnHeartBeat = function(data){
		var now = Date.now();
		lastNetHeartBeat = now;

		if(!data || (netMasterRang && data.rang < netMasterRang)){
			log.i('SKIP updating netMaster');
			return;
		}

		netMasterRang	= data.rang;
		netMaster		= data.netMaster;
		log.i('NETMASTER IS', peersIAM, data.netMaster);
	};

	self.isNetMaster = function(){
		return areTheSamePeers(netMaster, peersIAM);
	};

	self.isSysMaster = function(){
		return areTheSamePeers(sysMaster, peersIAM);
	};

	var peerGetOne = function(peer){
		if(peer instanceof Array){
			return peer[0];
		}
		return peer;
	};

	self.getAllConnections = function(){
		var conns = [];
		peerForEveryConnection(function(pc){
			conns.push({
					type	: pc.type,
					host	: peerGetOne(peersIAM),
					link	: peerGetOne(pc.peer)
				});
		});
		return conns;
	};

	self.whoIsIt = function(){
		return peersIAM.slice()[0];
	}

	self.setRang = function(r){
		peerMyRang = r;
	}

	// INITIALIZATION
	Step(
		function getiam(){
			cif.bind(this)
		},
		function bindconn(err, result){
			if(err || !result.peers){
				log.e(err, result.peers);
				throw 'whoami error';
			}

			peersIAM = result.peers;
			log.d('iam', result.peers);

			// here we use one connection interface for all incime msgs
			cif.on('data', srvGotMsg);
			cif.on('end', srvGotDisconnection);
			cif.on('error', srvGotDisconnection);

			this();
		},
		function sayhello(err){
			if(err){
				log.e(err);
				throw 'bind error';
			}

			// set me as ready to connect and load peer list
			peerUpdateHubAndConnect(this);
		},
		function finish(err){
			if(err){
				log.e('sayhello', err);
			}

			log.d('initialization complete');
			setInterval(myHeartBeat, HEARTBEAT_INTERVAL_MS);
			setInterval(checkMessageHashes, CHECK_HASHES_INTERVAL_MS);
		}
	);
}

function ConnMgr(){
	// dont forget 'new' operand
	if(!(this instanceof ConnMgr)){
		return new ConnMgr();
	}
	var self = this;
	var locked = false;

	self.lock = function(ms, callback){
		if(!locked){
			locked = true;
			callback();
			return;
		}

		async.until(
			function test(){
				if(locked === false){
					locked = true;
					return true;
				}
				return false;
			},
			function iter(cb){
				setTimeout(cb, 10);
			},
			function fin(){
				callback();
			}
		);
	};

	self.unlock = function(callback){
		locked =  false;
		callback();
	};

	self.get = function(callback){
		var rclient = redis.createClient();
		Step(
			function get(){
				rclient.lrange('peers', 0, 1, this);
			},
			function fin(err, peerslist){
				if(err){
					log.e(err);
				}

				rclient.quit();
				callback(null, peerslist);
			}
		);
	};

	self.add = function(peers, callback){
		log.d('PEER ADDED TO TOP', peers);
		var rclient = redis.createClient();
		Step(
			function get(){
				if(peers instanceof Array){
					var args = ['peers'].concat(peers);
					args.push(this);
					rclient.lpush.apply(rclient, args);
				}
				else{
					rclient.lpush('peers', peers, this);
				}
			},
			function fin(err){
				if(err){
					log.e(err);
				}

				rclient.quit();
				if(callback){
					callback();
				}
			}
		);
	};

	var masterInfo;

	self.getMaster = function(callback){
		callback(null, masterInfo);
	};

	self.setMaster = function(info, callback){
		masterInfo = info;
		callback();
	};
}

function Connection(){
	// dont forget 'new' operand
	if(!(this instanceof Connection)){
		return new Connection();
	}

	var self = this;
	var events = {};
	var server;
	var iam = [];
	// var NETWORK_MASK = '192.168.55';
	var NETWORK_MASK = '192.168.0.';

	var eventWrapper = function(client, event, iface, fn){
		if(event === 'data'){
			var data = '';
			client.on(event, function(chunk){
				data += chunk.toString();
				// split messages by NULL
				var msgs = data.split('\0');
				for(var i = 0; i < msgs.length; i++){
					var msg = msgs[i];
					if(msg && fn){
						fn(iface, msg);
					}
				}
				// leave last element (uncompleted)
				data = msg;
			});
		}
		else {
			client.on(event, function(a){
				if(fn){
					fn(iface, a);
				}
			});
		}
	};

	self.connect = function(peer, callback){
		var host = peer.split(':')[0];
		var port = peer.split(':')[1];

		var client = net.connect({ host: host, port: port }, function() {
			log.i('connected to server!');
		});

		var iface = {
			type : 'out',
			peer : peer,
			on   : function(event, fn){ eventWrapper(client, event, iface, fn); },
			send : function(msg, callback){ client.write(new Buffer(msg + '\0'), callback);	},
			end  : function(){ client.end(); }
		};

		callback(null, iface);
	};

	// used at bind (server connections)
	self.on = function(event, func){
		events[event] = func;
	};

	self.bind = function(callback){
		server = net.createServer(function(c) {
			// we got new connection ->
			log.d('client connected');
			var iface = {
				type : 'in',
				peer : undefined, // will be filled with hello
				send : function(msg, callback){ c.write(new Buffer(msg + '\0'), callback);	},
				end  : function() { c.end(); }
			};

			eventWrapper(c, 'data', iface, events['data']);
			eventWrapper(c, 'end', iface, events['end']);
			eventWrapper(c, 'error', iface, events['error']);
		});

		server.listen(0, function(){
			var sa = server.address();
			var port = sa.port;
			log.d('server bound', sa);

			var ifaceNames = os.networkInterfaces();

			for(var ifaceNameIdx in ifaceNames){
				var ifaceName = ifaceNames[ifaceNameIdx];
				for(var ifaceIdx in ifaceName){
					var iface = ifaceName[ifaceIdx];
					if(iface.internal || iface.family != 'IPv4'){
						continue;
					}
					if(iface.address.indexOf(NETWORK_MASK) === 0){
						iam.push([iface.address, port].join(':'));
					}
				}
			}

			callback(null, { peers : iam });
		});
	}
}

var connManager = new ConnMgr();
var gossips = [];

var instancesNum = 100;

async.until(
	function test(){
		return instancesNum === 0;
	},
	function iter(cb){
		instancesNum--;
		var connInterface = new Connection();
		var gossip = new Gossip(connManager, connInterface);
		gossips.push(gossip);
		setTimeout(cb, 5);
	},
	function fin(){
		log.i('fin');
	}
);

// GRAPH BUILDER PART
var rundir = process.cwd();
var srv = http.createServer(function (req, res) {

	var pobj = url.parse(req.url, true);
	var filename = path.join(rundir, pobj.pathname);

	if(filename.indexOf('/reset') > -1){
		log.i('CMD RESET:', pobj.query.node);
		gossips[0].sysMsg('rset', { peer : pobj.query.node }, true);
		res.end('OK');
		return;
	}

	if(filename.indexOf('/cmd') > -1){
		log.i('CMD:', pobj.query);
		gossips[0].message(
			{
				cmd : 'color',
				color : pobj.query.color
	  	},
	  	true
	  );
		res.end('OK');
		return;
	}

	if(filename.indexOf('/getstat') > -1){
		res.writeHead(200, {'Content-Type': 'application/json'});
		var data = [];

		for(var i = 0; i < gossips.length; i++){
			var gossip = gossips[i];

			var role = 'W'; // worker
			if(gossip.isSysMaster()){
				role = 'S'; // sysmaster
			}
			else if(gossip.isNetMaster()){
				role = 'N'; // netmaster
			}

			info = {
				type	: 'N', //node
				role	: role,
				host	: gossip.whoIsIt(),
				color	: gossip.color
			};

			data.push(info);

			var states = gossip.getAllConnections();

			for(var k = 0; k < states.length; k++){
				var conn = states[k];
				info = {
					type	: 'L', //node
					ctype	: conn.type,
					host	: conn.host,
					link	: conn.link
				};

				data.push(info);
			}
		}

		res.end(JSON.stringify(data));
		return;
	}

	// GENERAL HTTP FILE SERVER
	fs.exists(filename, function(exists) {
		if(!exists) {
			res.writeHead(404, {"Content-Type": "text/plain"});
			res.write("404 Not Found\n");
			res.end();
			return;
		}

		if(fs.statSync(filename).isDirectory()) filename += '/index.html';

		fs.readFile(filename, "binary", function(err, file) {
			if(err) {
				res.writeHead(500, {"Content-Type": "text/plain"});
				res.write(err + "\n");
				res.end();
				return;
			}

			res.writeHead(200);
			res.write(file, "binary");
			res.end();
		});
	});
});

srv.listen(8080, '127.0.0.1', function(err){
	if(err){
		log.e('BIND ERROR', err);
	}
});

// CONSOLE CONTROLS
var readline = require('readline'),
	rl = readline.createInterface(process.stdin, process.stdout);

rl.setPrompt('> ');
rl.prompt();

rl.on('line', function(line) {
  var args = line.split(' ');
  var cmd = args[0];

  switch(cmd) {
	case 'reset':
	  var who = args[1];
	  gossips[0].sysMsg('rset', who ? { peer : who } : undefined, true);
	  break;
	case 'rang':
	  var who = args[1];
	  var rang = +args[2];
	  gossips.forEach(function(g){
	  	if(g.whoIsIt() === who){
	  		console.log(who, 'seted rang to', rang);
	  		g.setRang(rang);
	  		return true;
	  	}
	  })
	  break;
	case 'color':
	  gossips[0].message({
		cmd : 'color',
		color : args[1]
	  }, true);
	  break;
	default:
	  console.log('Say what? ' + line.trim());
	  break;
  }
  rl.prompt();
}).on('close', function() {
  console.log('Have a great day!');
  process.exit(0);
});
