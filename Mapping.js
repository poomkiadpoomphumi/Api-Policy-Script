var imp = new JavaImporter(com.vordel.trace, java.util);

with (imp) {
    var col = [    
        "gmdr_scada_energy",
        "gmdr_client_energy",
        "scada_energy",
        "daily_billing_volume",
        "customer_type",
        "daily_billing_energy",
        "tpa_scada_volume",
        "meter_id",
        "scada_volume",
        "gmdr_scada_volume",
        "tpa_scada_energy",
        "monthly_billing_energy",
        "monthly_billing_volume",
        "datetime",
        "gmdr_client_volume",
        "scada_hv",
        "meter_name",
        "ogc_hv"
    ]; // Fixed columns

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
        var records = {};
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
                
                // Convert keys to uppercase
                record = convertKeysToUppercase(record);

                var _ = record["METER_NAME"]; // Group by meter_name
                if (!records[_]) {
                    records[_] = record;
                } else {
                    // If needed, handle merging of records for the same meter_name here
                    records[_] = record;
                }
            }

            // Construct final response
            msg.put("jsonRsp", JSON.stringify({ "RECORDS": records }));
            return true;
        } catch (e) {
            Trace.error("Error in invoke function: " + e.toString());
            msg.put("jsonRsp", JSON.stringify({ "Message": "An error occurred: " + e.toString() }));
            return false;
        }
    }
}
