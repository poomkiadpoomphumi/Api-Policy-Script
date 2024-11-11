var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        'datetime', 'c2p', 'nc5', 'n2', 'co2', 'lhvdry', 'c1', 'hv', 'c2',
        'c3', 'wi', 'c6', 'c7', 'sg', 'hhvsat', 'location', 'ic5', 'ic4',
        'c6p', 'hhvdry', 'nc4', 'dewpoint', 'uno', 'lhvsat', 'h2s', 'h2o'
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
        var val = msg.get("gc");
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
                    if (key === 'datetime') {
                        record[key] = formatDate(item.get(key));
                    } else if (key === 'location') {
                        record[key] = item.get("location").contains("current") ?
                                convertNullToEmptyString(item.get("location").split(".")[0]) :
                                convertNullToEmptyString(item.get("location"));
                    } else {
                        record[key] = convertNullToEmptyString(item.get(key));
                    }
                }

                records.push(record);
            }

            msg.put("jsonRsp", JSON.stringify({ "RECORDS": records }));
            return true;
        } catch (e) {
            Trace.error("Error in invoke function: " + e.toString());
            msg.put("jsonRsp", JSON.stringify({ "Message": "An error occurred: " + e.toString() }));
            return false;
        }
    }
}
