var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        "impact",
        "iscancel",
        "last_update",
        "jobdetail",
        "plancategory",
        "problem",
        "detailfix",
        "isrework",
        "status_workflow_name",
        "customergroup",
        "statusname",
        "totalperiod",
        "reason",
        "statations",
        "solution",
        "unitcode",
        "status_workflow_id",
        "ifix_technicianunitcode",
        "ifix_plannerunitcode",
        "plannergroup",
        "enddate",
        "ifix_plannercode",
        "cause",
        "notecancel",
        "statuscode",
        "ifix_techniciancode",
        "createdate",
        "customertype",
        "iscb",
        "startdate",
        "orderno",
        "functionname",
        "plancode"
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
                    if(key === 'statations'){
                        if(item.get(key) === ''){
                            record[key] = null;
                        }else{
                            record[key] = item.get(key);
                        }
                    }else{
                        record[key] = convertNullToEmptyString(item.get(key));
                    }
                    
                }

                records.push(convertKeysToUppercase(record));
            }

            msg.put("jsonRsp", JSON.stringify({"records":records}));
            return true;
        } catch (e) {
            Trace.error("Error in invoke function: " + e.toString());
            msg.put("jsonRsp", JSON.stringify({ "Message": "An error occurred: " + e.toString() }));
            return false;
        }
    }
}
