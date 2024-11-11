var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        'energy_hourly',
        'customer_type',
        'meter_end_date',
        'meter_id',
        'ng_billingtrans',
        'meter_point_id',
        'ref_tag_alarm_tpa',
        'oxygen',
        'hydrocarbon_dew_point',
        'ng_billingtrans_temp',
        'data_from',
        'meter_start_date',
        'moisture',
        'vw_ogc_hourly',
        'vw_ogc_daily',
        'ngbill_daily_update',
        'mercury',
        'flow_hourly',
        'sulfer'
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
        var val = msg.get("tpa_mapping_meter");
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
                    if(key === 'mercury'){
                        record[key] = null;
                    }else{
                        record[key] = convertNullToEmptyString(item.get(key));
                    }
                }

                records.push(convertKeysToUppercase(record));
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
