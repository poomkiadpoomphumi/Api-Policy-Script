var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        "room_has_catering",
        "room_tool",
        "room_status",
        "room_conference_system",
        "room_type_csv",
        "room_id",
        "room_has_conference",
        "room_detail",
        "room_name",
        "location_id",
        "room_permission"
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

            msg.put("jsonRsp", JSON.stringify({ "records": records }));
            return true;
        } catch (e) {
            Trace.error("Error in invoke function: " + e.toString());
            msg.put("jsonRsp", JSON.stringify({ "Message": "An error occurred: " + e.toString() }));
            return false;
        }
    }
}
