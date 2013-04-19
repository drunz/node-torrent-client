var bencode = require('./util/bencode');
var crypto = require("crypto");
var fs = require('fs');
var path = require('path');
var util = require('util');
var _ = require('underscore');

var PeerPool = require('./util/peerpool');
var ProcessUtils = require('./util/processutils');
var BitField = require('./util/bitfield');
var BufferUtils = require('./util/bufferutils');
var EventEmitter = require('events').EventEmitter;
var File = require('./file');
var Message = require('./message');
var Peer = require('./peer');
var Piece = require('./piece');
var Tracker = require('./tracker');

var LOGGER = require('log4js').getLogger('torrent.js');

var Torrent = function(client, clientId, port, file, downloadPath, peerLimit) {
  EventEmitter.call(this);

  this.client = client;
  this.clientId = clientId;
  this.port = port;

  this.downloadPath = downloadPath;

  this.downloaded = 0;
  this.uploaded = 0;

  this.leechers = 0;
  this.peers = {};
  this.pieces = {};
  this.seeders = 0;

  this.usePeerSelection = client.usePeerSelection;
  this.peerPool = new PeerPool("Torrent",   client.peerPool, 1, 1);

  this.isComplete = false;

  this.status = Torrent.STATUS_LOADING;

  var self = this;

  this.peerPool.on(PeerPool.APPROVED, function(peer) {
    self.addPeer(peer);
  });
  fs.readFile(file, 'binary', function(err, data) {
    if (err) {
      self.emit('error');
      self.status = Torrent.STATUS_LOAD_ERROR;
    } else {
      parse(self, data);
    }
  });
};
util.inherits(Torrent, EventEmitter);

Object.defineProperty(Torrent, 'STATUS_LOADING',     { value: 'loading',     enumerable: true });
Object.defineProperty(Torrent, 'STATUS_READY',       { value: 'ready',       enumerable: true });
Object.defineProperty(Torrent, 'STATUS_LOAD_ERROR',  { value: 'load_error',  enumerable: true });

// Incoming connections
Torrent.prototype.handleConnection = function(peer) {
  var self = this;

  if (self.peerPool.requestSlot()) {
    LOGGER.info(util.format('Adding new peer %s %s:%s', peer.peer_id, peer.ip, peer.port));

    peer.setTorrent(self);
    peer.on(Peer.DISCONNECT, function () {
      // we only need to release the slot
      // no priority peer selection for upload yet
      self.peerPool.releaseSlot();
    });
  }
  else if (self.usePeerSelection && self.peerPool.requestOverflowSlot()) {
    LOGGER.info(util.format('Too many peers (%s/%s), checking if %s:%s is any good', 
      _.size(self.peers), self.peerPool.peerLimit, peer.ip, peer.port));

    // check whether this is a better peer
    tryPeer(self, peer);
    peer.on(Peer.DISCONNECT, function () {
      self.peerPool.releaseOverflowSlot();
    });
  }
  else {
    LOGGER.info(util.format('Too many peers (%s/%s), rejecting to connect to %s:%s', 
      _.size(self.peers), self.peerPool.peerLimit, peer.ip, peer.port));
    peer.choke();
  }
};

Torrent.prototype.addPeer = function(peer) {
  if (!(peer.getIdentifier() in this.peers)) {
    this.peers[peer.getIdentifier()] = peer;
    var self = this;
    peer.on(Peer.CONNECT, function() {
      LOGGER.info('CONNECTED to ' + peer.address + ':' + peer.port);
      peer.sendMessage(new Message(Message.BITFIELD, self.bitfield.toBuffer()));
      LOGGER.info('Sent BITFIELD to ' + peer.address + ':' + peer.port);
    });
    peer.on(Peer.DISCONNECT, function() {
      LOGGER.info('DISCONNECTED from ' + peer.address + ':' + peer.port);
      peerDisconnect(self, peer);
      self.peerPool.releaseSlot();
    });
    peer.on(Peer.DONE, function() {
      LOGGER.info('DONE with' + peer.address + ':' + peer.port);
      peerDone(self, peer);
    });
    peer.on(Peer.CHOKED, function() {
      LOGGER.info('CHOKED by ' + peer.address + ':' + peer.port);
    });
    peer.on(Peer.READY, function() {
      //LOGGER.info('Torrent.addPeer [READY]');
      peerReady(self, peer);
    });
    peer.on(Peer.UPDATED, function() {
      var interested = peer.bitfield.xor(peer.bitfield.and(self.bitfield)).setIndices().length > 0;
      LOGGER.debug('UPDATED: ' + (interested ? 'interested' : 'not interested') + ' in ' + peer.address + ':' + peer.port);
      peer.setAmInterested(interested);
    });

    peer.setTorrent(this);
    peer.connect();
  }
};

Torrent.prototype.calculateDownloadRate = function() {
  var rate = 0;
  for (var id in this.peers) {
    rate += this.peers[id].currentDownloadRate;
  }
  return rate;
};

Torrent.prototype.calculateUploadRate = function() {
  var rate = 0;
  for (var id in this.peers) {
    rate += this.peers[id].currentUploadRate;
  }
  return rate;
};

Torrent.prototype.listPeers = function() {
  var peers = [];
  for (var id in this.peers) {
    var peer = this.peers[id];
    peers.push({
      address: peer.address,
      choked: peer.choked,
      requests: peer.numRequests,
      downloadRate: peer.currentDownloadRate,
      uploadRate: peer.currentUploadRate
    });
  }
  return peers;
};

Torrent.prototype.listTrackers = function() {
  var trackers = [];
  for (var i = 0; i < this.trackers.length; i++) {
    var tracker = this.trackers[i];
    trackers.push({
      state: tracker.state,
      error: tracker.errorMessage
    });
  }
  return trackers;
};

Torrent.prototype.removePeer = function(peer) {
  peer.removeAllListeners(Peer.CHOKED);
  peer.removeAllListeners(Peer.CONNECT);
  peer.removeAllListeners(Peer.DISCONNECT);
  peer.removeAllListeners(Peer.READY);
  peer.removeAllListeners(Peer.UPDATED);
  delete this.peers[peer.getIdentifier()];
  LOGGER.debug(util.format('Removing peer %s %s', peer.peerId, peer.getIdentifier()));
};

Torrent.prototype.requestChunk = function(index, begin, length, cb) {
  var self = this;
  var piece = this.pieces[index];
  if (piece) {
    piece.getData(begin, length, function(err, data) {
      if (err) {
        cb(err);
      }
      else {
        self.uploaded += (data && data.length) ? data.length : 0;
        cb(null, data);
      }
    });
  } else {
    cb();
  }
};

Torrent.prototype.start = function() {
  var self = this;
  for (var i = 0; i < this.trackers.length; i++) {
    this.trackers[i].start((function(tracker) {
      return function(data) {
        trackerUpdated(self, tracker, data);
      };
    })(this.trackers[i]));
  }
};

Torrent.prototype.stop = function(cb) {
  var self = this;
  cb = cb || function() {};
  if (this.status===Torrent.STATUS_READY) {
    for (var i = 0; i < this.trackers.length; i++) {
      this.trackers[i].stop();
    }
    for (var id in this.peers) {
      var peer = this.peers[id];
      peer.disconnect('Torrent stopped.');
    }
    // go through each file in the torrent and close them
    var files = self.files.slice();
    (function _close() {
      if (files.length == 0) {
        return cb();
      }
      var file = files.shift();
      file.close(function(err) {
        if (err) {
          LOGGER.warn('could not close file ' + file.path + ': ' + err);
        }
        process.nextTick(_close);
      })
    })();
  }
  else {
    cb();
  }
};

Torrent.prototype.trackerInfo = function() {
  return {
    info_hash: this.infoHash,
    peer_id: this.clientId,
    port: this.port,
    uploaded: this.uploaded,
    downloaded: this.downloaded,
    left: this.size
  };
};

function createFiles(self, rawTorrent, cb) {
  self.files = [];
  if (rawTorrent.info.length) {
      var length = rawTorrent.info.length;
      self.files.push(new File(path.join(self.downloadPath, self.name), length, null, function(err) {
        if (err) {
          throw new Error('Error creating file, err = ' + err);
        }
        self.size = length;
        cb();
      }));
  } else {
    var basePath = path.join(self.downloadPath, self.name);
    fs.exists(basePath, function(exists) {
      function doCreate() {
        var files = rawTorrent.info.files;
        self.size = 0;
        var offset = 0;
        (function nextFile() {
          if (files.length === 0) {
            cb();
          } else {
            var file = files.shift();
            (function checkPath(curPath, pathArr) {
              if (pathArr.length == 1) {
                self.files.push(new File(path.join(curPath, pathArr[0]),
                  file.length, offset, function(err) {
                    if (err) {
                      return cb(new Error('Error creating file, err = ' + err));
                    }
                    self.size += file.length;
                    offset += file.length;
                    //LOGGER.debug("torrent.createFiles: nextTick nextFile");
                    ProcessUtils.nextTick(nextFile);
                  }));
              }
              else {
                curPath = path.join(curPath, pathArr.shift());
                fs.exists(curPath, function(curPathExists) {
                  if (!curPathExists) {
                    fs.mkdir(curPath, 0777, function(err) {
                      if (err) {
                        return cb(new Error("Couldn't create directory. err = " + err));
                      }
                      checkPath(curPath, pathArr);
                    });
                  } else {
                    checkPath(curPath, pathArr);
                  }
                });
              }
            })(basePath, file.path.slice(0));
          }
        })();
      }
      if (!exists) {
        fs.mkdir(basePath, 0777, function(err) {
          if (err) {
            return cb(new Error("Couldn't create directory. err = " + err));
          }
          doCreate();
        });
      } else {
        doCreate();
      }
    });
  }
}

function createPieces(self, hashes, cb) {
  self.pieces = [];
  var numPieces = hashes.length / 20;
  var index = 0;
  (function create() {
    if (index === numPieces) {
      LOGGER.debug('Finished validating pieces.  Number of valid pieces = '
        + self.bitfield.cardinality()
        + ' out of a total of '
        + self.bitfield.length);
      cb(self.bitfield.cardinality() === self.bitfield.length);
    } else {
      var hash = hashes.substr(index * 20, 20);
      var pieceLength = self.pieceLength;
      if (index == numPieces - 1) {
        pieceLength = self.size % self.pieceLength;
      }
      var piece = new Piece(index, index * self.pieceLength, pieceLength, hash,
        self.files, function(piece) {
        if (piece.isComplete) {
          self.bitfield.set(piece.index);
        } else {
          piece.once(Piece.COMPLETE, function(piece) {
            pieceComplete(self, piece);
          });
        }
        index++;
        create();
      });
      self.pieces[index] = piece;
    }
  })();
}

function createTrackers(self, announce, announceList) {
  self.trackers = [];
  var announceMap = {};
  if (announceList) {
    for (var i = 0; i < announceList.length; i++) {
      announceMap[announceList[i][0]] = 0;
    }
  }
  announceMap[announce] = 0;

  var url = require('url');
  for (var j in announceMap) {
    self.trackers.push(new Tracker(j, self));
  }
}

function parse(self, data) {

  var rawTorrent;

  try {
    rawTorrent = bencode.decode(data);

    self.createdBy = rawTorrent['created by'];
    self.creationDate = rawTorrent['creation date'];
    self.name = rawTorrent.info.name;
    self.pieceLength = rawTorrent.info['piece length'];

    self.infoHash = new Buffer(crypto.createHash('sha1')
      .update(bencode.encode(rawTorrent.info))
      .digest(), 'binary');

    var pieceHashes = rawTorrent.info.pieces;
    self.bitfield = new BitField(pieceHashes.length / 20);
    self.activePieces = new BitField(self.bitfield.length);

    createTrackers(self, rawTorrent.announce, rawTorrent['announce-list']);

    createFiles(self, rawTorrent, function(err) {
      if (err) {
        self.last_error = err;
        self.emit('error');
      }
      else {
        createPieces(self, pieceHashes, function(complete) {
          if (complete) {
            LOGGER.info('torrent already complete');
            self.isComplete = true;
          }
          self.status = Torrent.STATUS_READY;
          self.emit('ready');
        });
      }
    });
  }
  catch(err) {
    self.status = Torrent.STATUS_LOAD_ERROR;
    self.last_error = err;
    self.emit('error');
  }
}

// TODO: review disconnect
function peerDisconnect(self, peer) {
  var activePieces = self.activePieces.setIndices();

  // deactivate any piece that the peer was leeching
  for (var i = 0; i < activePieces.length; i++) {
    var index = activePieces[i];
    if (peer.pieces[index]) {
      self.activePieces.unset(index);
    }
  }
  // clear pieces on peer ???
  peer.pieces = {};
}

function peerDone(self, peer) {
  self.removePeer(peer);
}

function peerReady(self, peer) {
  var activePieces = self.activePieces.setIndices();
  var piece;

  // find an active piece for the peer
  for (var i = 0; i < activePieces.length; i++) {
    var index = activePieces[i];
    var p = self.pieces[index];
    // if peer is currently leeching the piece and not all chunk requested yet, select the piece
    if (peer.pieces[index] &&
        !p.hasRequestedAllChunks()) {
      piece = p;
      break;
    }
  }

  if (!piece) {
    // if no active piece found, pick a new piece and activate it

    // available = peerhas ^ (peerhas & (active | completed))
    var available = peer.bitfield.xor(
      peer.bitfield.and(
        self.activePieces.or(self.bitfield)));

    // pick a random piece out of the available ones
    var set = available.setIndices();
    var index = set[Math.round(Math.random() * (set.length - 1))];
    if (index !== undefined) {
      piece = self.pieces[index];
      self.activePieces.set(index);
    }
  }
  if (piece) {
    // if we have a piece, request chunks for it
    //LOGGER.debug('Peer is ready, requesting piece ' + piece.index);
    peer.requestPiece(piece);
  } else if (peer.numRequests === 0) {
    LOGGER.debug('No available pieces for peer ' + peer.getIdentifier());
    peer.setAmInterested(false);
  }
}

function pieceComplete(self, piece) {
  LOGGER.debug('Piece complete, piece index = ' + piece.index);

  self.bitfield.set(piece.index);
  self.downloaded += piece.length;

  self.emit('progress', self.bitfield.cardinality() / self.bitfield.length) //when more file is down...

  if (self.bitfield.cardinality() === self.bitfield.length) {
    LOGGER.info('torrent download complete');
    self.isComplete = true;
    self.emit('complete');
  }

  var have = new Message(Message.HAVE, BufferUtils.fromInt(piece.index));
  for (var i in self.peers) {
    var peer = self.peers[i];
    if (peer.initialised) {
      peer.sendMessage(have);
    }
  }

  self.activePieces.unset(piece.index);
}

function tryPeer(self, peer) {
  var score = peer.getScore();
  var lowest = self.peerPool.discard();

  if (lowest) {
    var lowestScore = lowest.getScore();

    if (score > lowestScore) {
      LOGGER.info("Found a better peer %s:%s (score=%s), replacing with worst known (score=%s)...",
        peer.address, peer.port, score, lowestScore);

      lowest.choke();
      self.peerPool.add(peer);
    }
    else {
      LOGGER.info("This peer %s:%s (score=%s) is not any better, getting rid of it...",
        peer.address, peer.port, score);

      peer.choke();
      self.peerPool.add(lowest); // add back and trigger pool update
    }
  }
}

function trackerUpdated(self, tracker, data) {

  if(data) { // data will be null if a valid response from tracker was not received
    var seeders = data.seeders;
    if (tracker.seeders) {
      self.seeders -= tracker.seeders;
    }
    tracker.seeders = seeders;
    if (tracker.seeders) {
      self.seeders += tracker.seeders;
    }

    var leechers = data.leechers;
    if (tracker.leechers) {
      self.leechers -= tracker.leechers;
    }
    tracker.leechers = leechers;
    if (tracker.leechers) {
      self.leechers += tracker.leechers;
    }

    // only initiate connections with peers if the local data is incomplete
    if (!self.isComplete && data.peers) {
      for (var i = 0; i < data.peers.length; i++) {
        var peerData = data.peers[i];
        if (!self.peers[Peer.getIdentifier(peerData)]) {
          var peer = new Peer(peerData.peer_id, peerData.ip, peerData.port);
          self.peerPool.requestSlot(peer);
        }
      }
    }
  }

  self.emit('updated');
}

module.exports = Torrent;
