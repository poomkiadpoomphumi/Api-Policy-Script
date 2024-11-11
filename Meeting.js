var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        "meeting_department_name",
        "meeting_datetime_start",
        "meeting_description",
        "meeting_reserver_phone",
        "meeting_catering_detail",
        "meeting_reserver_name",
        "meeting_update_timestamp",
        "meeting_insert_timestamp",
        "meeting_room_type",
        "meeting_title",
        "meeting_insert_employee_id",
        "meeting_update_employee_id",
        "room_id",
        "meeting_conference_detail",
        "meeting_conference_app",
        "meeting_participant_count",
        "meeting_datetime_end"
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
