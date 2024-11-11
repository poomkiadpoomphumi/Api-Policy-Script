var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        'workorderno',
        'workpermit_no',
        'workpermit_date',
        'create_date',
        'create_by',
        'remark'
    ]; // fixed columns

    function convertNullToEmptyString(val) {
        return val == null ? "" : val.toString();
    }
    function convertKeysToUppercase(record) {
        var newRecord = {};
        for (var key in record) {
            if (record.hasOwnProperty(key)) {
                newRecord[key.toUpperCase()] = record[key];
            }
        }
        return newRecord;
    }
    function invoke(msg) {
        var records = [];
        var val = msg.get("obj");
        var count = parseInt(msg.get("db.result.count"), 10);

        if (isNaN(count) || count === 0) {
            msg.put("jsonRsp", JSON.stringify({ "Message": "No Data available." }));
            return true;
        }

        try {
            for (var i = 0; i < val.size(); i++) {
                var item = val.get(i);
                var record = {};
                for (var j = 0; j < col.length; j++) {
                    var key = col[j];
                    record[key] = convertNullToEmptyString(item.get(key));
                }

                records.push(convertKeysToUppercase(record));
            }

            msg.put("jsonRsp", JSON.stringify({"RECORDS":records}));
            return true;
        } catch (e) {
            Trace.error("Error in invoke function: " + e.toString());
            msg.put("jsonRsp", JSON.stringify({ "Message": "An error occurred: " + e.toString() }));
            return false;
        }
    }
}
