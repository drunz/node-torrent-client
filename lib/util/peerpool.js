var _ = require("underscore");
var PriorityQueue = require('priorityqueuejs');

var PeerPool = function(name, parentPool, peerLimit, overflowPeerLimit) {
  this.name = name;
  this.parentPool = parentPool || null;
  
  this.activePeers = 0;
  this.overflowActivePeers = 0;
  this.peerLimit = peerLimit;
  this.overflowPeerLimit = overflowPeerLimit;

  this.peers = {};
  this.queue = new PriorityQueue(function(a, b) {
    return b.getScore() - a.getScore(); // we want min priority queue
  });
};

// PeerPool.prototype.requestSlot = function (torrent, peer) {
//   if (this.activeSlots < this.maxSlots) {
//     this.queue.enq({
//       peerid:    peer.peerId,
//       torrentid: torrent.infoHash,
//       score:     Math.sqrt(peer.uploadrate + peer.downloadrate);
//     });
//     this.pool[torrent.infoHash] = peer;

//     this.activeSlots++;
//   }
// };

// PeerPool.prototype.releaseSlot = function (torrent, peer) {
//   delete this.pool[torrent.infoHash];
//   this.activeSlots--;
// };

PeerPool.prototype.add = function (peer) {
  this.queue.enq(peer);
}

PeerPool.prototype.min = function () {
  if (!this.queue.isEmpty())
    return this.queue.peek();
  else
    return null;
}

PeerPool.prototype.discard = function () {
  if (!this.queue.isEmpty())
    return this.queue.deq();
  else
    return null;
}

//
// Requesting upload/download/overflow slots
//

PeerPool.prototype._request = function(type) {
  var self = this;

  console.log("%s: %d/%d slots in use (overflow: %d/%d)...", 
    self.name,
    self.activePeers, self.peerLimit, 
    self.overflowActivePeers, self.overflowPeerLimit);

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

  if (self.peerLimit === 0 ? true : (activePeers < peerLimit)) {
    if (self.parentPool) {

      var parentApproved = self.parentPool._request(type);
      if (parentApproved) {
        switch (type) {
          case PeerPool.REQ_STANDARD:
            self.activePeers++;
            break;
          case PeerPool.REQ_OVERFLOW:
            self.overflowActivePeers++;
            break;
        }
        return true;
      }
      return false; // parent did not approve
    }
    return true; // no parent
  }
  return false; // we could not approve
}

PeerPool.prototype.requestSlot = function() {
  return this._request(PeerPool.REQ_STANDARD);
};

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
};

PeerPool.prototype.releaseSlot = function() {
  this._release(PeerPool.REQ_STANDARD);
};

PeerPool.prototype.releaseOverflowSlot = function() {
  this._release(PeerPool.REQ_OVERFLOW);
};


PeerPool.REQ_STANDARD = 0;
PeerPool.REQ_OVERFLOW = 1;


module.exports = PeerPool;