var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        "pl36_ben_tan",
        "pl42_pressure",
        "pl34_erp_prp",
        "pl42_jda",
        "pl42_ecp_erp",
        "pl36_bv2_1",
        "pl34_pressure",
        "pl42_bv3_1",
        "pl24_fe_5031",
        "pl42_plt2",
        "date",
        "pl34_erw_plt1",
        "pl24_onshore",
        "pl36_fe_005",
        "pl24_pressure",
        "pl36_pressure",
        "pl24_fe_5030",
        "pl36_ecp_prp",
        "pl42_prp",
        "pl34_bv1"
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

            msg.put("jsonRsp", JSON.stringify({ "records": records }));
            return true;
        } catch (e) {
            Trace.error("Error in invoke function: " + e.toString());
            msg.put("jsonRsp", JSON.stringify({ "Message": "An error occurred: " + e.toString() }));
            return false;
        }
    }
}
