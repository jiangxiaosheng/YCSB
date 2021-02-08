package site.ycsb.db.rocksdb;

/**
 * Interface for ReplyListeners monitoring inputstream from Replicator.
 */

interface ReplyListener {
  void onEvent(Reply reply);
  //boolean opMatch(String op);
}
