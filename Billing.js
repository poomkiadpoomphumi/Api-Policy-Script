var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        'first_created', 'to_date', 'client_fid', 'last_updated', 'from_date',
        'client_type', 'hv', 'mass', 'energy', 'bmonth', 'sg', 'volume', 'client_name',
        'client_id'
    ]; // fixed columns

    var dtFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    function convertNullToEmptyString(val) {
        return val == null ? "" : val.toString().trim();
    }

    function formatDate(timestamp) {
        if (timestamp == null) return "";
        var dt = new Date(isNaN(parseInt(timestamp, 10)) ? 0 : parseInt(timestamp, 10));
        return dtFormat.format(dt);
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
        var val = msg.get("operate");
        var count = parseInt(msg.get("db.result.count"), 10);

        if (isNaN(count) || count === 0) {
            msg.put("jsonRsp", JSON.stringify({ "Message": "No Data available." }));
            return true;
        }

        try {
            if (val != null) {
                for (var i = 0; i < val.length; i++) {
                    var item = val[i];
                    var record = {};
                    for (var j = 0; j < col.length; j++) {
                        var key = col[j];
                        var value = item.get(key);
                        if (key === 'from_date' || key === 'first_created' || key === 'to_date' 
                            || key === 'bmonth' || key === 'last_updated') {
                            record[key === 'first_created' ? 'first_create' : key] = formatDate(value);
                        } else {
                            record[key] = convertNullToEmptyString(value);
                        }
                    }
                    records.push(convertKeysToUppercase(record));
                }
            }
            msg.put("jsonRsp", JSON.stringify({ "records": records }, null, 2));
            return true;
        } catch (e) {
            Trace.error("Error in invoke function: " + e.toString());
            msg.put("jsonRsp", JSON.stringify({ "Message": "An error occurred: " + e.toString() }));
            return false;
        }
    }
}
