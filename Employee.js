var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
            "engname",
            "head_code",
            "code",
            "unit_level_name",
            "head_position",
            "remark",
            "project_level_id",
            "lname_eng",
            "iname",
            "dummy_relationship",
            "lname",
            "unitname",
            "iname_eng",
            "costcenter",
            "fname",
            "unitabbr",
            "longname",
            "project_level_name",
            "unitcode",
            "emailaddr",
            "poscode",
            "mobile",
            "fname_eng",
            "unit_level_id",
            "business_unit_id",
            "posname",
            "business_unit_name",
            "lastupdate"
    ]; // fixed columns

    function convertNullToEmptyString(val) {
        return val == null ? "" : val.toString();
    }
    function invoke(msg) {
        var records = [];
        var val = msg.get("obj");
        var count = parseInt(msg.get("db.result.count"), 10);

        if (isNaN(count) || count === 0) {
            msg.put("jsonRsp", "No Data available.");
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
