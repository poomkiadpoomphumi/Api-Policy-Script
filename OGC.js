var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        'ghvdry', 'nc4', 'ghvsat', 'wi', 'nc5', 'ic5', 'uno', 
        'nhvdry', 'co2', 'ic4', 'sg', 'c7', 'c1', 'c2', 'location', 
        'c3', 'n2', 'c6', 'datetime'
    ]; // fixed columns

    function convertNullToEmptyString(val) {
        return val == null ? "" : val.toString();
    }

    function formatDate(params) {
        var dt = new Date(parseInt(params, 10));
        var dtFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
        var val = msg.get("ogc");
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
                    }else if(key === 'ghvsat'){
                        record['ghysat'] = convertNullToEmptyString(item.get(key));
                    } else {
                        record[key] = convertNullToEmptyString(item.get(key));
                    }
                }

                records.push(convertKeysToUppercase(record));
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
