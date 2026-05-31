package com.sparkx.diff

class BloomDiffStrategySuite extends DiffSuiteBase with DiffStrategyBehaviors {
  override def strategy: DiffStrategy = new BloomDiffStrategy(
    expectedNumItems = 1000L,
    fpp = 0.01
  )
  override def strategyName: String = "BloomDiff"
}
