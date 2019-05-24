Changelog
=========


2.0.1 (2019-05-24)
------------------

**Bug fixes**

- ``pool_size`` setting should remain optional


2.0.0 (2019-02-08)
------------------

**Breaking changes**

- Upgrade to Kinto >= 13 storage API.


1.3.0 (2018-04-26)
------------------

- Update storage tests with new Kinto 9.x features. (#13)


1.2.0 (2017-08-17)
------------------

- Cache set now requires a ttl value
- Cache delete() method now returns the deleted value
- Cache never accept to store bytes.


1.1.0 (2017-02-23)
------------------

- Upgrade to last storage, permissions and cache backends features. (#7)


1.0.1 (2016-08-18)
------------------

**Bug fixes**

- Fix compability with Kinto 4.0 about unique fields (Kinto/kinto#763)


1.0.0 (2016-08-11)
------------------

- Move the kinto redis backends to an external repository.
