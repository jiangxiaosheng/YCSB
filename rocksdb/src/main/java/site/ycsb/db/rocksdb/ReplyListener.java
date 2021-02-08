package site.ycsb.db.rocksdb;

/**
 * Interface for reply listeners.
 */

interface ReplyListener {
  void onEvent(Reply reply);
}