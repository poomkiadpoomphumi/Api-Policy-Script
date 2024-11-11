var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        "scada_sg",
        "ogc_hv",
        "ogc_gdry",
        "scada_wi",
        "ogc_un",
        "ogc_ic4",
        "ogc_ic5",
        "scada_volume",
        "scada_gdry",
        "ogc_ndry",
        "meter_name",
        "scada_nc5",
        "scada_nc4",
        "scada_c6",
        "ogc_c2",
        "ogc_c1",
        "ogc_co2",
        "ogc_c6",
        "scada_c2",
        "scada_c3",
        "ogc_c3",
        "gmdr_client_energy",
        "monthly_billing_energy_prev1",
        "daily_billing_volume",
        "monthly_billing_energy_prev2",
        "gmdr_scada_energy",
        "monthly_billing_energy",
        "scada_energy",
        "tpa_scada_volume",
        "customer_type",
        "monthly_billing_volume_prev2",
        "ogc_wi",
        "monthly_billing_volume",
        "ogc_sg",
        "scada_un",
        "gmdr_scada_volume",
        "tpa_scada_energy",
        "ogc_nc5",
        "monthly_billing_volume_prev1",
        "scada_c1",
        "scada_ic5",
        "ogc_nc4",
        "scada_ic4",
        "scada_co2",
        "scada_ndry",
        "gasday",
        "daily_billing_energy",
        "scada_n2",
        "gmdr_client_volume",
        "ogc_n2",
        "scada_hv",
        "meter_id"
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
