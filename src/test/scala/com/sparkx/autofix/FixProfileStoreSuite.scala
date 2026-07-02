package com.sparkx.autofix

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FixProfileStoreSuite extends AnyFunSuite with Matchers {

  private def tempStore(): FixProfileStore = {
    val dir = java.nio.file.Files.createTempDirectory("sparkx-autofix-test").toFile
    dir.deleteOnExit()
    new FixProfileStore(dir.toURI.toString, new Configuration())
  }

  test("save then load round-trips a profile with hints and attempts") {
    val store = tempStore()
    val profile = FixProfile(
      fingerprint  = "deadbeef",
      sample       = "SELECT a FROM t JOIN u ON t.k = u.k",
      status       = FixProfile.Optimizing,
      baselineMs   = Some(1000),
      bestMs       = Some(420),
      bestHints    = Seq(BroadcastHint(Seq("u")), RepartitionHint(64)),
      pendingHints = Seq(RepartitionHint(128)),
      attempts     = Seq(
        FixAttempt(Nil, 1000, 111L, improved = false),
        FixAttempt(Seq(BroadcastHint(Seq("u"))), 420, 222L, improved = true)
      ),
      updatedTs    = 999L
    )
    store.save(profile)

    val loaded = store.load("deadbeef")
    loaded shouldBe defined
    loaded.get.fingerprint shouldBe "deadbeef"
    loaded.get.bestHints shouldBe Seq(BroadcastHint(Seq("u")), RepartitionHint(64))
    loaded.get.pendingHints shouldBe Seq(RepartitionHint(128))
    loaded.get.attempts.map(_.durationMs) shouldBe Seq(1000, 420)
    loaded.get.attempts(1).hints shouldBe Seq(BroadcastHint(Seq("u")))
  }

  test("loading an unknown fingerprint returns None") {
    tempStore().load("nope") shouldBe None
  }

  test("list returns all saved profiles") {
    val store = tempStore()
    store.save(FixProfile.initial("aaa", "q1"))
    store.save(FixProfile.initial("bbb", "q2"))
    store.list().map(_.fingerprint).toSet shouldBe Set("aaa", "bbb")
  }
}
