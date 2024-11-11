var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        "date",
        "quality_spec",
        "nod",
        "solving",
        "time_of_occurence",
        "description",
        "duration",
        "witnessed_by",
        "nor",
        "notification_no",
        "is_deleted",
        "causes",
        "platform_id",
        "type_of_quality",
        "immediate_action",
        "id",
        "value"
    ]; // fixed columns

    function convertNullToEmptyString(val) {
        return val == null ? "" : val.toString();
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

                records.push(record);
            }

            msg.put("jsonRsp", JSON.stringify(records));
            return true;
        } catch (e) {
            Trace.error("Error in invoke function: " + e.toString());
            msg.put("jsonRsp", JSON.stringify({ "Message": "An error occurred: " + e.toString() }));
            return false;
        }
    }
}
