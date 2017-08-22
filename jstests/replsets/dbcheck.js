/**
 * dbcheck.js
 *
 * Test the dbCheck command.
 */

(function() {
    "use strict";

    let nodeCount = 3;
    let replSet = new ReplSetTest({name: "dbCheckSet", nodes: nodeCount});

    replSet.startSet();
    replSet.initiate();

    let dbName = "dbCheck-test";
    let collName = "dbcheck-collection";
    let otherCollName = "dbcheck-other-collection";

    // Insert some meaningless documents into both collections.
    function addSomeDocuments() {
        let db = replSet.getPrimary().getDB(dbName);
        for (let i = 0; i < 50; ++i) {
            db[collName].insertOne({"i": NumberLong(i)});
            db[otherCollName].insertOne({"j": 50 - i});
        }
    };

    // Clear local.system.healthlog.
    function clearLog(conn) {
        conn.getDB("local").system.healthlog.drop();
    }

    addSomeDocuments();

    // Wait for dbCheck to complete (on both primaries and secondaries).  Fails an assertion if
    // dbCheck takes longer than maxMs.
    function awaitDbCheckCompletion(db, maxMs) {
        let start = Date.now();

        if (maxMs === undefined) {
            maxMs = 10000;
        }

        function dbCheckCompleted() {
            return db.currentOp().inprog.filter((x) => x["desc"] == "dbCheck")[0] === undefined;
        }

        assert.soon(dbCheckCompleted, "dbCheck timed out", maxMs, 50);
        replSet.awaitSecondaryNodes();
        replSet.awaitReplication();
    }

    // Check that everything in the health log shows a successful and complete check with no found
    // inconsistencies.
    function checkLogAllConsistent(conn) {
        let healthlog = conn.getDB("local").system.healthlog;

        assert(healthlog.find().count(), "dbCheck put no batches in health log");

        let maxResult = healthlog.aggregate([
            {$match: {operation: "dbCheckBatch"}},
            {$group: {_id: 1, key: {$max: "$data.maxKey"}}}
        ]);

        assert(maxResult.hasNext(), "dbCheck put no batches in health log");
        assert.eq(maxResult.next().key, {"$maxKey": 1}, "dbCheck batches should end at MaxKey");

        let minResult = healthlog.aggregate([
            {$match: {operation: "dbCheckBatch"}},
            {$group: {_id: 1, key: {$min: "$data.minKey"}}}
        ]);

        assert(minResult.hasNext(), "dbCheck put no batches in health log");
        assert.eq(minResult.next().key, {"$minKey": 1}, "dbCheck batches should start at MinKey");

        // Assert no errors (i.e., found inconsistencies).
        let errs = healthlog.find({"severity": {"$ne": "info"}});
        if (errs.hasNext()) {
            assert(false, "dbCheck found inconsistency: " + tojson(errs.next()));
        }

        // Assert to failures (i.e., checks that failed to complete).
        let failedChecks = healthlog.find({"operation": "dbCheckBatch", "data.success": false});
        if (failedChecks.hasNext()) {
            assert(false, "dbCheck batch failed: " + tojson(failedChecks.next()));
        }

        // Finds an entry with data.minKey === MinKey, and then matches its maxKey against
        // another document's minKey, and so on, and then checks that the result of that search
        // has data.maxKey === MaxKey.
        let completeCoverage = healthlog.aggregate([
            {$match: {
                "operation": "dbCheckBatch",
                "data.minKey": MinKey
            }},
            {$graphLookup: {
                from: "system.healthlog",
                startWith: "$data.minKey",
                connectToField: "data.minKey",
                connectFromField: "data.maxKey",
                as: "batchLimits",
                restrictSearchWithMatch: { "operation": "dbCheckBatch" }
            }},
            {$match: { "batchLimits.data.maxKey": MaxKey }}
        ]);

        assert(completeCoverage.hasNext(), "dbCheck batches do not cover full key range");
    }

    // Check that the total of all batches in the health log on `conn` is equal to the total number
    // of documents and bytes in `coll`.
    function checkTotalCounts(conn, coll) {
        let healthlog = conn.getDB("local").system.healthlog;

        let result = healthlog.aggregate([
            { $match: {
                "operation": "dbCheckBatch"
            }},
            { $group: {
                "_id": null,
                "totalDocs": { $sum: "$data.count" },
                "totalBytes": { $sum: "$data.bytes" }
            }}
        ]).next();

        assert.eq(result.totalDocs, coll.count(), "dbCheck batches do not count all documents");
        assert.eq(result.totalBytes, coll.dataSize(), "dbCheck batches do not count all bytes");
    }

    // First check behavior when everything is consistent.
    function simpleTestConsistent() {
        let master = replSet.getPrimary();

        addSomeDocuments();

        clearLog(master);
        clearLog(master);
        addSomeDocuments();

        assert.neq(master, undefined);
        let db = master.getDB(dbName);
        assert.commandWorked(db.runCommand({"dbCheck": collName}));

        awaitDbCheckCompletion(db);

        checkLogAllConsistent(master);
        checkTotalCounts(master, db[collName]);

        for (let secondary of replSet.getSecondaries()) {
            checkLogAllConsistent(secondary, true);
            checkTotalCounts(secondary, secondary.getDB(dbName)[collName]);
        }
    }

    // Same thing, but now with concurrent updates.
    function concurrentTestConsistent() {
        let master = replSet.getPrimary();

        let db = master.getDB(dbName);

        // Add enough documents that dbCheck will take a few seconds.
        db[collName].insertMany([...Array(10000).keys()].map((x) => ({i: x})));

        assert.commandWorked(db.runCommand({"dbCheck": collName}));

        let coll = db[collName];
        while (db.currentOp().inprog.filter((x) => x["desc"] === "dbCheck").length) {
            coll.updateOne({}, { "$inc": { "i": 10 }});
            coll.insertOne({ "i": 42 });
            coll.deleteOne({});
        }

        awaitDbCheckCompletion(db);

        checkLogAllConsistent(master);
        // Omit check for total counts, which might have changed with concurrent udpates.

        for (let secondary of replSet.getSecondaries()) {
            checkLogAllConsistent(secondary, true);
        }
    }

    simpleTestConsistent();
    concurrentTestConsistent();
})();
