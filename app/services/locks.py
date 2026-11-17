import threading

# 正式轨与影子轨共用的写互斥锁。
# 两侧争用同一批分区/裁决时，写临界区必须串行、且提交在锁内：
# 不加锁会出现「影子双成功并把正式轨盖掉」的交错写入。
STORAGE_WRITE_LOCK = threading.RLock()
