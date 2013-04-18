var _ = require("underscore");
var PriorityQueue = require('priorityqueuejs');

var PeerPool = function(name, peerLimit, parentPool) {
  this.name = name;
  this.parentPool = parentPool || null;
  
  this.activePeers = 0;
  this.peerLimit = peerLimit;
  this.overflowActivePeers = 0;
  this.overflowPeerLimit   = 1;

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


// PeerPool.prototype._request= function(type) {
//   switch (type) {
//     case PeerPool.REQ_STANDARD:
//       return this.
//       break;
//     case PeerPool.REQ_OVERFLOW:
//       break:
//   }
// }

PeerPool.prototype.requestSlot = function() {
  console.log("%s: %d/%d slots in use (overflow: %d/%d)...", this.name,
    this.activePeers, this.peerLimit, 
    this.overflowActivePeers, this.overflowPeerLimit);

  if (this.peerLimit === 0 ? true : (this.activePeers < this.peerLimit)) {
    if (this.parentPool) {
      var parentApproved = this.parentPool.requestSlot();
      if (parentApproved) {
        this.activePeers++;
        return true;
      }
      return false;
    }
    return true;
  }
  return false;
};

PeerPool.prototype.requestOverflowSlot = function() {
  console.log("%s: %d/%d slots in use (overflow: %d/%d)...", this.name,
    this.activePeers, this.peerLimit, 
    this.overflowActivePeers, this.overflowPeerLimit);

  if (this.peerLimit === 0 ? true : (this.overflowActivePeers < this.overflowPeerLimit)) {
    if (this.parentPool) {
      var parentApproved = this.parentPool.requestOverflowSlot();
      if (parentApproved) {
        this.activePeers++;
        return true;
      }
      return false;
    }
    return true;
  }
  return false;
};

PeerPool.prototype.releaseSlot = function() {
  this.activePeers--;

  console.log("%s: %d/%d slots in use (overflow: %d/%d)...", this.name,
    this.activePeers, this.peerLimit, 
    this.overflowActivePeers, this.overflowPeerLimit);
};

PeerPool.prototype.releaseOverflowSlot = function() {
  this.overflowActivePeers--;

  console.log("%s: %d/%d slots in use (overflow: %d/%d)...", this.name,
    this.activePeers, this.peerLimit, 
    this.overflowActivePeers, this.overflowPeerLimit);
};

// PeerPool.REQ_STANDARD = 0;
// PeerPool.REQ_OVERFLOW = 0;

module.exports = PeerPool;