package com.sparkx.diff

class MD5DiffStrategySuite extends DiffSuiteBase with DiffStrategyBehaviors {
  override def strategy: DiffStrategy = new MD5DiffStrategy()
  override def strategyName: String = "MD5Diff"
}
