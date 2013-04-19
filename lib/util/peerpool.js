var _ = require('underscore');
var util = require('util');
var async = require('async');
var PriorityQueue = require('priorityqueuejs');
var EventEmitter = require('events').EventEmitter;


var PeerPool = function(name, parentPool, peerLimit, overflowPeerLimit) {
  EventEmitter.call(this);

  this.name = name;
  this.parentPool = parentPool || null;
  
  this.activePeers = 0;
  this.overflowActivePeers = 0;
  this.peerLimit = peerLimit;
  this.overflowPeerLimit = overflowPeerLimit;

  this.lastEvict = null;
  this.interval = 30e3;
  this.pending = [];
  this.retries = {};

  this.queue = new PriorityQueue(function(a, b) {
    return b.getScore() - a.getScore(); // we want min priority queue
  });

  var self = this;

  setTimeout(function() { _processPending(self) }, self.interval);
};
util.inherits(PeerPool, EventEmitter);

function _processPending(self) {
  console.log("%s: Processing (%d) pending slot requests...", 
    self.name, self.pending.length);

  var n = self.pending.length;
  var tries = 0;

  while (tries < n) {
    var peer = self.pending.shift();

    if (!(peer.peer_id in self.retries))
      self.retries[peer.peer_id] = 1;
    else
      self.retries[peer.peer_id]++;

    console.log("%s: Trying peer %s:%s %d-th time", 
    self.name, peer.address, peer.port, self.retries[peer.peer_id]);

    self.requestSlot(peer);
    tries++;
  }

  setTimeout(function() { _processPending(self) }, self.interval);
};

//
// Managing active peer pool
//

PeerPool.prototype.add = function (peer) {
  this.queue.enq(peer);
};

PeerPool.prototype.min = function () {
  if (!this.queue.isEmpty())
    return this.queue.peek();
  else
    return null;
};

PeerPool.prototype.discard = function () {
  if (!this.queue.isEmpty())
    return this.queue.deq();
  else
    return null;
};

//
// Managing pending peer queue
// - Pending peers will be processed every 30 seconds
//   using an optimistic unchoking-like algorithm.
//

PeerPool.prototype.addPending = function (peer) {
  this.pending.push(peer);
}

//
// Requesting upload/download/overflow slots
//

PeerPool.prototype._takeslot = function(type) {
  switch (type) {
    case PeerPool.REQ_STANDARD:
      this.activePeers++;
      break;
    case PeerPool.REQ_OVERFLOW:
      this.overflowActivePeers++;
      break;
  }
};

PeerPool.prototype._request = function(type) {
  var self = this;

  var activePeers;
  var peerLimit;

  switch (type) {
    case PeerPool.REQ_STANDARD:
      activePeers = self.activePeers;
      peerLimit   = self.peerLimit;
      break;
    case PeerPool.REQ_OVERFLOW:
      activePeers = self.overflowActivePeers;
      peerLimit   = self.overflowPeerLimit;
      break;
  }

  var approved = false;

  if (self.peerLimit === 0 ? true : (activePeers < peerLimit)) {
    if (self.parentPool) {
      var parentApproved = self.parentPool._request(type);
      if (parentApproved) {
        self._takeslot(type);
        approved = true;
      }
    } 
    else {
      self._takeslot(type);
      approved = true;
    }
  }

  console.log("%s: %s, %d/%d slots in use (overflow: %d/%d)...", 
    self.name,
    approved ? "Accepted" : "Declined",
    self.activePeers, self.peerLimit, 
    self.overflowActivePeers, self.overflowPeerLimit);

  return approved;
}

PeerPool.prototype.requestSlot = function(peer) {
  console.log("%s: Slot request for peer %s:%s", 
    this.name, peer.address, peer.port);

  var approved = this._request(PeerPool.REQ_STANDARD);

  if (approved) {
    delete this.retries[peer.peer_id];
    this.emit(PeerPool.APPROVED, peer);
  }
  else {
    this.pending.push(peer);
    console.log("%s: Queued peer %s:%s (Pending: %d)", 
      this.name, peer.address, peer.port,
      this.pending.length);

    this.emit(PeerPool.QUEUED, peer);
  }

  return approved;
};

// if (self.peerPool.requestSlot(peer)) {
//   LOGGER.info(util.format('Adding new peer %s %s:%s', 
//     peerData.peer_id, peerData.ip, peerData.port));

//   peer.once(Peer.DISCONNECT, function () {
//     self.peerPool.releaseSlot();
//   });
//   self.peerPool.add(peerObj);
//   // self.addPeer(peer); // TODO: stop passing full torrent through to peer
// }
// else if (self.usePeerSelection && self.peerPool.requestOverflowSlot()) {
//   LOGGER.info(util.format('Too many peers (%s/%s), checking if %s:%s is any good', 
//     _.size(self.peers), self.peerPool.peerLimit, peer.ip, peer.port));

//   // check whether this is a better peer
//   var peerObj = new Peer(peer.peer_id, peer.ip, peer.port, self);
//   peerObj.once(Peer.CONNECT, function () {
//     tryPeer(self, peerObj);
//   });
//   peerObj.once(Peer.DISCONNECT, function () {
//     self.peerPool.releaseOverflowSlot();
//   });
// }
// else {
//   LOGGER.info(util.format('Too many peers (%s/%s), rejecting to connect to %s:%s', 
//     _.size(self.peers), self.peerPool.peerLimit, peer.ip, peer.port));

//   // if we dont get slots here, we want to be in a queue in order to make sure
//   // we will get a fair chance before the first torrents get a new chance to request for slots!
// }

PeerPool.prototype.requestOverflowSlot = function() {
  return this._request(PeerPool.REQ_OVERFLOW);
};

//
// Releasing upload/download/overflow slots
//

PeerPool.prototype._release = function(type) {
  var self = this;

  switch (type) {
    case PeerPool.REQ_STANDARD:
      self.activePeers--;
      break;
    case PeerPool.REQ_OVERFLOW:
      self.overflowActivePeers--;
      break;
  }

  console.log("%s: %d/%d slots in use (overflow: %d/%d)...", 
    self.name,
    self.activePeers, self.peerLimit, 
    self.overflowActivePeers, self.overflowPeerLimit);

  self.parentPool._release(type);
};

PeerPool.prototype.releaseSlot = function() {
  this._release(PeerPool.REQ_STANDARD);
  this.emit(PeerPool.SLOT_AVAILABLE);
};

PeerPool.prototype.releaseOverflowSlot = function() {
  this._release(PeerPool.REQ_OVERFLOW);
  this.emit(PeerPool.OVERFLOW_SLOT_AVAILABLE);
};


PeerPool.REQ_STANDARD = 0;
PeerPool.REQ_OVERFLOW = 1;

PeerPool.SLOT_AVAILABLE = 'slot_available';
PeerPool.OVERFLOW_SLOT_AVAILABLE = 'overflow_slot_available';
PeerPool.APPROVED = 'approved';
PeerPool.QUEUED = 'queued';


module.exports = PeerPool;