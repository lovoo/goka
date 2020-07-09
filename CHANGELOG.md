# Changelog

## [Unreleased](https://github.com/lovoo/goka/tree/HEAD)

[Full Changelog](https://github.com/lovoo/goka/compare/v0.9.0-beta3...HEAD)

**Implemented enhancements:**

- Migration guide [\#249](https://github.com/lovoo/goka/issues/249)
- View: expose view and connection state [\#248](https://github.com/lovoo/goka/issues/248)
- PartitionTable backoff on errors [\#247](https://github.com/lovoo/goka/issues/247)

**Closed issues:**

- Examples `simplest` and `clicks` panic with "non-positive interval for NewTicker" [\#258](https://github.com/lovoo/goka/issues/258)
- panic: non-positive interval for NewTicker [\#255](https://github.com/lovoo/goka/issues/255)

**Merged pull requests:**

- fixed broken goka blogpost link [\#257](https://github.com/lovoo/goka/pull/257) ([frairon](https://github.com/frairon))

## [v0.9.0-beta3](https://github.com/lovoo/goka/tree/v0.9.0-beta3) (2020-04-09)

[Full Changelog](https://github.com/lovoo/goka/compare/v0.9.0-beta2...v0.9.0-beta3)

**Implemented enhancements:**

- Add blocking recovered method [\#80](https://github.com/lovoo/goka/issues/80)
- Emit messages with headers [\#77](https://github.com/lovoo/goka/issues/77)

**Closed issues:**

- cannot run multiple consumers in the same group [\#204](https://github.com/lovoo/goka/issues/204)

**Merged pull requests:**

- stats-tracking improved [\#245](https://github.com/lovoo/goka/pull/245) ([frairon](https://github.com/frairon))
- return trackOutput if stats are nil [\#244](https://github.com/lovoo/goka/pull/244) ([R053NR07](https://github.com/R053NR07))
- added lots of godoc, fixed many linter errors, added Open call when c… [\#243](https://github.com/lovoo/goka/pull/243) ([frairon](https://github.com/frairon))
- Open Storage in PartitionTable when performing Setup [\#242](https://github.com/lovoo/goka/pull/242) ([frairon](https://github.com/frairon))
- updated readme for configuration, added changelog [\#240](https://github.com/lovoo/goka/pull/240) ([frairon](https://github.com/frairon))

## [v0.9.0-beta2](https://github.com/lovoo/goka/tree/v0.9.0-beta2) (2020-03-20)

[Full Changelog](https://github.com/lovoo/goka/compare/v0.9.0-beta1...v0.9.0-beta2)

## [v0.9.0-beta1](https://github.com/lovoo/goka/tree/v0.9.0-beta1) (2020-03-19)

[Full Changelog](https://github.com/lovoo/goka/compare/v0.1.4...v0.9.0-beta1)

**Implemented enhancements:**

- Update Kafka consumer group library [\#187](https://github.com/lovoo/goka/issues/187)

**Closed issues:**

- Does goka support kafka 2.2.1 [\#233](https://github.com/lovoo/goka/issues/233)
- Go get error [\#213](https://github.com/lovoo/goka/issues/213)
- Samara-cluster dependency seems to have an issue [\#211](https://github.com/lovoo/goka/issues/211)
- Dropping message while rebalancing [\#207](https://github.com/lovoo/goka/issues/207)
- Lost lots of messages due to async produce [\#192](https://github.com/lovoo/goka/issues/192)
- processor gets stuck in shutdown after error [\#176](https://github.com/lovoo/goka/issues/176)
- Proposal: support static partitioning [\#163](https://github.com/lovoo/goka/issues/163)
- Proposal: add tracing support [\#160](https://github.com/lovoo/goka/issues/160)

## [v0.1.4](https://github.com/lovoo/goka/tree/v0.1.4) (2020-01-22)

[Full Changelog](https://github.com/lovoo/goka/compare/v0.1.3...v0.1.4)

**Closed issues:**

- How can I debug a "negative WaitGroup counter" in a processor? [\#198](https://github.com/lovoo/goka/issues/198)
- View is empty, if log forced the keys/values come back [\#197](https://github.com/lovoo/goka/issues/197)
- Question: Rebalancing callback? [\#195](https://github.com/lovoo/goka/issues/195)

**Merged pull requests:**

- Remove unused channels [\#215](https://github.com/lovoo/goka/pull/215) ([heltonmarx](https://github.com/heltonmarx))
- Adding header on message [\#214](https://github.com/lovoo/goka/pull/214) ([toninho09](https://github.com/toninho09))
- repeating messages from input stream after recover [\#208](https://github.com/lovoo/goka/pull/208) ([WideLee](https://github.com/WideLee))
- revert in memory locks [\#203](https://github.com/lovoo/goka/pull/203) ([TheL1ne](https://github.com/TheL1ne))
- protect storage writes [\#200](https://github.com/lovoo/goka/pull/200) ([TheL1ne](https://github.com/TheL1ne))
- Tester tolerate unconsumed queues [\#199](https://github.com/lovoo/goka/pull/199) ([TheL1ne](https://github.com/TheL1ne))

## [v0.1.3](https://github.com/lovoo/goka/tree/v0.1.3) (2019-06-28)

[Full Changelog](https://github.com/lovoo/goka/compare/v0.1.2...v0.1.3)

**Fixed bugs:**

- Using a tester with an emitter doesn't seem to be working [\#188](https://github.com/lovoo/goka/issues/188)

**Merged pull requests:**

- feature 195: Add a rebalance callback to the processor. [\#196](https://github.com/lovoo/goka/pull/196) ([gmather](https://github.com/gmather))

## [v0.1.2](https://github.com/lovoo/goka/tree/v0.1.2) (2019-06-18)

[Full Changelog](https://github.com/lovoo/goka/compare/v0.1.1...v0.1.2)

**Implemented enhancements:**

- Proposal: log progress of recovery [\#182](https://github.com/lovoo/goka/issues/182)
- start logging while recovering [\#183](https://github.com/lovoo/goka/pull/183) ([TheL1ne](https://github.com/TheL1ne))

**Closed issues:**

- Lookup not working with multi-partitioned topics [\#186](https://github.com/lovoo/goka/issues/186)
- How can I config multiple consumer for 1 Topic [\#184](https://github.com/lovoo/goka/issues/184)
- multi\_iterator looks broken [\#178](https://github.com/lovoo/goka/issues/178)
- Rebalancing error [\#172](https://github.com/lovoo/goka/issues/172)

**Merged pull requests:**

- bugfix \#188: tester for emitter [\#190](https://github.com/lovoo/goka/pull/190) ([frairon](https://github.com/frairon))
- storage: merge iterator implementation [\#185](https://github.com/lovoo/goka/pull/185) ([SamiHiltunen](https://github.com/SamiHiltunen))
- 178 multi\_iterator looks broken [\#180](https://github.com/lovoo/goka/pull/180) ([frairon](https://github.com/frairon))
- Groupgraphhook option [\#175](https://github.com/lovoo/goka/pull/175) ([frairon](https://github.com/frairon))
- Tester deadlock workaround [\#173](https://github.com/lovoo/goka/pull/173) ([frairon](https://github.com/frairon))

## [v0.1.1](https://github.com/lovoo/goka/tree/v0.1.1) (2019-01-29)

[Full Changelog](https://github.com/lovoo/goka/compare/v0.1.0...v0.1.1)

**Implemented enhancements:**

- Improve tester implementation [\#145](https://github.com/lovoo/goka/issues/145)

**Fixed bugs:**

- Updates broke go get [\#152](https://github.com/lovoo/goka/issues/152)
- zookeeper address with trailing "/" fails [\#126](https://github.com/lovoo/goka/issues/126)
- Running processor without prior topics fails [\#125](https://github.com/lovoo/goka/issues/125)

**Closed issues:**

- How to improve performance? [\#164](https://github.com/lovoo/goka/issues/164)
- Question: Scanning a Joined Topic [\#159](https://github.com/lovoo/goka/issues/159)
- Removal of db7/kazoo-go and subsequent repo deletion breaks previous builds [\#154](https://github.com/lovoo/goka/issues/154)
- error on recovering view does not restart [\#141](https://github.com/lovoo/goka/issues/141)
- review errors to fail on [\#140](https://github.com/lovoo/goka/issues/140)
- Processor failures: expected rebalance OK but received rebalance error [\#131](https://github.com/lovoo/goka/issues/131)
- unable to get goka running with the following error.  [\#119](https://github.com/lovoo/goka/issues/119)

**Merged pull requests:**

- fixed bindata for web templates [\#170](https://github.com/lovoo/goka/pull/170) ([frairon](https://github.com/frairon))
- add logo [\#169](https://github.com/lovoo/goka/pull/169) ([frairon](https://github.com/frairon))
- add Context\(\) method to goka.Context [\#166](https://github.com/lovoo/goka/pull/166) ([db7](https://github.com/db7))
- terminate tester when one of the queue consumers died [\#165](https://github.com/lovoo/goka/pull/165) ([frairon](https://github.com/frairon))
- fix integration tests [\#162](https://github.com/lovoo/goka/pull/162) ([frairon](https://github.com/frairon))
- fixes wrong commit for queue tracker [\#158](https://github.com/lovoo/goka/pull/158) ([frairon](https://github.com/frairon))
- remove proto and go-metrics direct dependencies [\#157](https://github.com/lovoo/goka/pull/157) ([db7](https://github.com/db7))
- add ensure config to check topic [\#155](https://github.com/lovoo/goka/pull/155) ([db7](https://github.com/db7))
- clean trailing slashes in zookeeper chroot [\#153](https://github.com/lovoo/goka/pull/153) ([db7](https://github.com/db7))
- update mocks to latest sarama [\#150](https://github.com/lovoo/goka/pull/150) ([db7](https://github.com/db7))
- ensure topic exists as more flexible topic creation option [\#149](https://github.com/lovoo/goka/pull/149) ([db7](https://github.com/db7))
- fix processor tests [\#148](https://github.com/lovoo/goka/pull/148) ([db7](https://github.com/db7))
- cleanup processor rebalance [\#147](https://github.com/lovoo/goka/pull/147) ([db7](https://github.com/db7))
- improve tester implementation [\#146](https://github.com/lovoo/goka/pull/146) ([frairon](https://github.com/frairon))
- use default kazoo-go [\#144](https://github.com/lovoo/goka/pull/144) ([db7](https://github.com/db7))

## [v0.1.0](https://github.com/lovoo/goka/tree/v0.1.0) (2018-08-03)

[Full Changelog](https://github.com/lovoo/goka/compare/4dd3c74e427f580a08953ad5cb704dc4421a6426...v0.1.0)

**Implemented enhancements:**

- Proposal: replace Start\(\) and Stop\(\) with Run\(context.Context\) [\#120](https://github.com/lovoo/goka/issues/120)
- Support badgeDB [\#55](https://github.com/lovoo/goka/issues/55)
- Question: Accessing a goka.View concurrently? [\#53](https://github.com/lovoo/goka/issues/53)
- Remove snapshot from storage [\#30](https://github.com/lovoo/goka/issues/30)

**Fixed bugs:**

- Prevent offset commit in case of panic [\#129](https://github.com/lovoo/goka/issues/129)
- Failing view was not added [\#113](https://github.com/lovoo/goka/issues/113)
- Don't check copartitioning for lookup topics [\#109](https://github.com/lovoo/goka/issues/109)
- Getting error from Emitter [\#103](https://github.com/lovoo/goka/issues/103)
- Initial offset handling [\#97](https://github.com/lovoo/goka/issues/97)
- Allow passing custom hasher for graph dependencies [\#84](https://github.com/lovoo/goka/issues/84)
- monitoring panics on stateless processor [\#62](https://github.com/lovoo/goka/issues/62)
- a view never becomes ready on an empty topic [\#35](https://github.com/lovoo/goka/issues/35)
- access group table without goka.Persist\(...\) [\#18](https://github.com/lovoo/goka/issues/18)

**Closed issues:**

- View should have option to fallback to local-only mode [\#102](https://github.com/lovoo/goka/issues/102)
- Question: Same topic as input and join [\#95](https://github.com/lovoo/goka/issues/95)
- Proposal: Add joins with watermarks [\#94](https://github.com/lovoo/goka/issues/94)
- Goka is production ready? [\#93](https://github.com/lovoo/goka/issues/93)
- Issues getting value from view [\#83](https://github.com/lovoo/goka/issues/83)
- HowToX : Is it possible to iterate on View with a subset ?  [\#82](https://github.com/lovoo/goka/issues/82)
- Proposal: Implement message keys as bytes instead of strings [\#81](https://github.com/lovoo/goka/issues/81)
- Provide examples of lookups [\#79](https://github.com/lovoo/goka/issues/79)
- goka does not work latest bsm/sarama-cluster [\#68](https://github.com/lovoo/goka/issues/68)
- Reduce metrics overhead [\#57](https://github.com/lovoo/goka/issues/57)
- panic: runtime error: invalid memory address or nil pointer dereference [\#49](https://github.com/lovoo/goka/issues/49)
- Consume nil messages should be an option [\#41](https://github.com/lovoo/goka/issues/41)
- Organizing/modeling Views [\#32](https://github.com/lovoo/goka/issues/32)
- print error on failure right away [\#10](https://github.com/lovoo/goka/issues/10)
- panic on error. like a double-close [\#2](https://github.com/lovoo/goka/issues/2)

**Merged pull requests:**

- Fix passing opts to LevelDB in BuilderWithOptions [\#139](https://github.com/lovoo/goka/pull/139) ([mhaitjema](https://github.com/mhaitjema))
- Expose partition and offset on the callback context [\#136](https://github.com/lovoo/goka/pull/136) ([burdiyan](https://github.com/burdiyan))
- typo fix in tester code [\#135](https://github.com/lovoo/goka/pull/135) ([frairon](https://github.com/frairon))
- bugfix tester, improve emitter and mock [\#134](https://github.com/lovoo/goka/pull/134) ([frairon](https://github.com/frairon))
- make iteration order deterministic [\#133](https://github.com/lovoo/goka/pull/133) ([db7](https://github.com/db7))
- if context panics, finish context with error [\#130](https://github.com/lovoo/goka/pull/130) ([db7](https://github.com/db7))
- Feature/run context [\#127](https://github.com/lovoo/goka/pull/127) ([db7](https://github.com/db7))
- fixing multi-iterator test cases [\#122](https://github.com/lovoo/goka/pull/122) ([db7](https://github.com/db7))
- fix only first two iters checked [\#121](https://github.com/lovoo/goka/pull/121) ([j0hnsmith](https://github.com/j0hnsmith))
- Feature/move kafkamock [\#118](https://github.com/lovoo/goka/pull/118) ([db7](https://github.com/db7))
- trying with go 1.9 [\#117](https://github.com/lovoo/goka/pull/117) ([db7](https://github.com/db7))
- Update mocks [\#116](https://github.com/lovoo/goka/pull/116) ([db7](https://github.com/db7))
- Adding TopicManagerBuilderWithConfig to allow custom Sarama config [\#115](https://github.com/lovoo/goka/pull/115) ([andrewmunro](https://github.com/andrewmunro))
- mostly complying with gometalinter [\#114](https://github.com/lovoo/goka/pull/114) ([db7](https://github.com/db7))
- Feature/errgroup [\#111](https://github.com/lovoo/goka/pull/111) ([db7](https://github.com/db7))
- Allow non-copartitioned lookup tables [\#110](https://github.com/lovoo/goka/pull/110) ([db7](https://github.com/db7))
- guarantee partition goroutines finished before stop\(\) returns [\#107](https://github.com/lovoo/goka/pull/107) ([db7](https://github.com/db7))
- fix embarrasing dead unlock [\#106](https://github.com/lovoo/goka/pull/106) ([db7](https://github.com/db7))
- add restartable view support [\#105](https://github.com/lovoo/goka/pull/105) ([db7](https://github.com/db7))
- bugfix \#103: propagate error in Emitter.EmitSync [\#104](https://github.com/lovoo/goka/pull/104) ([frairon](https://github.com/frairon))
- consume streams from newest [\#100](https://github.com/lovoo/goka/pull/100) ([db7](https://github.com/db7))
- Bugfix/fix stalled partitions [\#98](https://github.com/lovoo/goka/pull/98) ([db7](https://github.com/db7))
- added redis storage option [\#96](https://github.com/lovoo/goka/pull/96) ([heltonmarx](https://github.com/heltonmarx))
- Make builders builders [\#92](https://github.com/lovoo/goka/pull/92) ([db7](https://github.com/db7))
- forward options to lookup views [\#91](https://github.com/lovoo/goka/pull/91) ([db7](https://github.com/db7))
- finished monitoring-example readme [\#90](https://github.com/lovoo/goka/pull/90) ([frairon](https://github.com/frairon))
- add testcase for regression testing of already fixed issue 18. Resolv… [\#89](https://github.com/lovoo/goka/pull/89) ([frairon](https://github.com/frairon))
- messaging example [\#88](https://github.com/lovoo/goka/pull/88) ([db7](https://github.com/db7))
- added support for Seek on iterator and IteratorWithRange on storage [\#85](https://github.com/lovoo/goka/pull/85) ([jbpin](https://github.com/jbpin))
- Bugfix/stalled partition recovery [\#78](https://github.com/lovoo/goka/pull/78) ([frairon](https://github.com/frairon))
- initial time in stats [\#76](https://github.com/lovoo/goka/pull/76) ([db7](https://github.com/db7))
- stop partition while marking recovered [\#75](https://github.com/lovoo/goka/pull/75) ([db7](https://github.com/db7))
- Confluent kafka go [\#74](https://github.com/lovoo/goka/pull/74) ([db7](https://github.com/db7))
- use default channel size from sarama in kafka package [\#73](https://github.com/lovoo/goka/pull/73) ([db7](https://github.com/db7))
- Join web views [\#71](https://github.com/lovoo/goka/pull/71) ([frairon](https://github.com/frairon))
- support for new notifications in sarama-cluster [\#70](https://github.com/lovoo/goka/pull/70) ([SamiHiltunen](https://github.com/SamiHiltunen))
- speed up stats collection [\#69](https://github.com/lovoo/goka/pull/69) ([db7](https://github.com/db7))
- use AsyncClose in simple consumer [\#67](https://github.com/lovoo/goka/pull/67) ([db7](https://github.com/db7))
- stop-partition bugfix [\#66](https://github.com/lovoo/goka/pull/66) ([frairon](https://github.com/frairon))
- return stats immediately when partition marking storage recovered [\#65](https://github.com/lovoo/goka/pull/65) ([db7](https://github.com/db7))
- forward partition consumer errors [\#64](https://github.com/lovoo/goka/pull/64) ([SamiHiltunen](https://github.com/SamiHiltunen))
- web/monitoring: removed tableName in template which fails on statelesss processors and was unused. Fixes \#62 [\#63](https://github.com/lovoo/goka/pull/63) ([frairon](https://github.com/frairon))
- reset stats after load finished [\#61](https://github.com/lovoo/goka/pull/61) ([db7](https://github.com/db7))
- web-components regenerated, refresh-interval reduced [\#60](https://github.com/lovoo/goka/pull/60) ([frairon](https://github.com/frairon))
- Webinterface [\#59](https://github.com/lovoo/goka/pull/59) ([frairon](https://github.com/frairon))
- Lightweight partition stats [\#58](https://github.com/lovoo/goka/pull/58) ([db7](https://github.com/db7))
- change table suffix to -table [\#54](https://github.com/lovoo/goka/pull/54) ([db7](https://github.com/db7))
- removing codec from storage [\#52](https://github.com/lovoo/goka/pull/52) ([db7](https://github.com/db7))
- deletes partition consumer from map even on errors [\#51](https://github.com/lovoo/goka/pull/51) ([db7](https://github.com/db7))
- nil pointer panic when closing producer  [\#50](https://github.com/lovoo/goka/pull/50) ([db7](https://github.com/db7))
- disable partition consumption delay once done [\#47](https://github.com/lovoo/goka/pull/47) ([db7](https://github.com/db7))
- timestamp passing in simple consumer [\#46](https://github.com/lovoo/goka/pull/46) ([SamiHiltunen](https://github.com/SamiHiltunen))
- nil handling options [\#45](https://github.com/lovoo/goka/pull/45) ([db7](https://github.com/db7))
- methods for checking recovery status [\#44](https://github.com/lovoo/goka/pull/44) ([SamiHiltunen](https://github.com/SamiHiltunen))
- properly stop goroutines on Close\(\) [\#43](https://github.com/lovoo/goka/pull/43) ([db7](https://github.com/db7))
- Evict method for Views [\#42](https://github.com/lovoo/goka/pull/42) ([SamiHiltunen](https://github.com/SamiHiltunen))
- return after deleting in DefaultUpdate [\#40](https://github.com/lovoo/goka/pull/40) ([SamiHiltunen](https://github.com/SamiHiltunen))
- Ignore nil message [\#39](https://github.com/lovoo/goka/pull/39) ([db7](https://github.com/db7))
- deletion semantics [\#38](https://github.com/lovoo/goka/pull/38) ([SamiHiltunen](https://github.com/SamiHiltunen))
- passing client id to builders [\#37](https://github.com/lovoo/goka/pull/37) ([db7](https://github.com/db7))
- let producerBuilder set partitioner in sarama.Config [\#36](https://github.com/lovoo/goka/pull/36) ([db7](https://github.com/db7))
- simplify context and move set offset to context commit [\#34](https://github.com/lovoo/goka/pull/34) ([db7](https://github.com/db7))
- view iterator [\#33](https://github.com/lovoo/goka/pull/33) ([SamiHiltunen](https://github.com/SamiHiltunen))
- snapshot and batching removal, recovery transactions [\#31](https://github.com/lovoo/goka/pull/31) ([SamiHiltunen](https://github.com/SamiHiltunen))
- iterator offset key skipping [\#29](https://github.com/lovoo/goka/pull/29) ([SamiHiltunen](https://github.com/SamiHiltunen))
- Expose message timestamp and metrics improvements [\#28](https://github.com/lovoo/goka/pull/28) ([SamiHiltunen](https://github.com/SamiHiltunen))
- option to provide metrics registry [\#27](https://github.com/lovoo/goka/pull/27) ([SamiHiltunen](https://github.com/SamiHiltunen))
- Bugfix/shutdown on errors [\#26](https://github.com/lovoo/goka/pull/26) ([frairon](https://github.com/frairon))
- logger interface and options [\#25](https://github.com/lovoo/goka/pull/25) ([SamiHiltunen](https://github.com/SamiHiltunen))
- examples [\#24](https://github.com/lovoo/goka/pull/24) ([frairon](https://github.com/frairon))
- Readme update [\#23](https://github.com/lovoo/goka/pull/23) ([db7](https://github.com/db7))
- Dependency fixes [\#22](https://github.com/lovoo/goka/pull/22) ([SamiHiltunen](https://github.com/SamiHiltunen))
- Readme [\#19](https://github.com/lovoo/goka/pull/19) ([frairon](https://github.com/frairon))
- isolate concurrent gets in snapshot [\#17](https://github.com/lovoo/goka/pull/17) ([db7](https://github.com/db7))
- Fail in partition view [\#16](https://github.com/lovoo/goka/pull/16) ([db7](https://github.com/db7))
- expect emit in correct order. Added expectall-function [\#15](https://github.com/lovoo/goka/pull/15) ([frairon](https://github.com/frairon))
- inputs for multiple topics [\#14](https://github.com/lovoo/goka/pull/14) ([frairon](https://github.com/frairon))
- renaming producer to emitter [\#13](https://github.com/lovoo/goka/pull/13) ([db7](https://github.com/db7))
- String types for Table, Stream and Group [\#12](https://github.com/lovoo/goka/pull/12) ([db7](https://github.com/db7))
- Sync recover [\#11](https://github.com/lovoo/goka/pull/11) ([db7](https://github.com/db7))
- adding lookup tables [\#9](https://github.com/lovoo/goka/pull/9) ([db7](https://github.com/db7))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
