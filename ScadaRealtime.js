var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        'status_state',
        'time',
        'intg_day_previous',
        'flag_fresh',
        'tagname',
        'intg_day_current'
    ]; // fixed columns

    function convertNullToEmptyString(val) {
        return val == null ? "" : val.toString();
    }

    function formatDate(params) {
        var dt = new Date(parseInt(params, 10));
        var dtFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dtFormat.format(dt);
    }

    function invoke(msg) {
        var records = [];
        var val = msg.get("counter");
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
                    if (key === 'time') {
                        record[key] = formatDate(item.get(key));
                    } else {
                        record[key] = convertNullToEmptyString(item.get(key));
                    }
                }

                records.push(record);
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
