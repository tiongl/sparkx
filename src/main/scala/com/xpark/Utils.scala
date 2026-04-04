package com.xpark

object Utils {
  def formatBytes(bytes: Long): String = {
    if (bytes >= (1L << 30))      f"${bytes.toDouble / (1L << 30)}%.2f GB"
    else if (bytes >= (1L << 20)) f"${bytes.toDouble / (1L << 20)}%.2f MB"
    else if (bytes >= (1L << 10)) f"${bytes.toDouble / (1L << 10)}%.2f KB"
    else                          s"${bytes} B"
  }

  def formatDuration(ms: Long): String = {
    if (ms >= 3600000)      f"${ms.toDouble / 3600000}%.2f h"
    else if (ms >= 60000)   f"${ms.toDouble / 60000}%.2f min"
    else if (ms >= 1000)    f"${ms.toDouble / 1000}%.2f s"
    else                    s"${ms} ms"
  }
}
