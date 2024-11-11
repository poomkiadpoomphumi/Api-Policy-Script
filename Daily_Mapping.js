var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
    var col = [
        'gmdr_scada_back_2',
        'gmdr_scada_main_4',
        'gmdr_client_back_3',
        'quality_point_scada_c3',    
        'gmdr_client_main_5',
        'gmdr_client_back_4',
        'quality_point_scada_c1',   
        'gmdr_scada_back_5',
        'quality_point_scada_uno',  
        'quality_point_scada_nhvdry',
        'realtime_scada_energy_tag_1',
        'gmdr_client_back_5',
        'quality_point_scada_nc4',
        'realtime_scada_energy_unit',
        'realtime_scada_volume_tag_1',
        'gmdr_scada_ssvt_unit',
        'realtime_scada_volume_tag_6',
        'gmdr_client_back_2',
        'gmdr_scada_back_3',
        'quality_point_name',
        'gmdr_scada_main_1',
        'realtime_scada_energy_tag_3',
        'quality_point_scada_wi',
        'gmdr_scada_back_4',
        'realtime_scada_energy_tag_6',
        'gmdr_scada_back_1',
        'daily_client_id',
        'gmdr_client_ssvt_unit',
        'quality_point_scada_ghvsat',
        'realtime_scada_volume_unit',
        'gmdr_scada_main_6',
        'gmdr_client_main_2',
        'quality_point_scada_c7',
        'quality_point_scada_ghvdry',
        'realtime_scada_volume_tag_2',
        'gmdr_scada_main_3',
        'gmdr_client_main_3',
        'realtime_scada_volume_tag_5',
        'quality_point_scada_co2',
        'realtime_scada_data_type',
        'gmdr_client_back_6',
        'quality_point_ogc',
        'gmdr_client_et_unit',
        'quality_point_id',
        'gmdr_scada_main_5',
        'realtime_scada_volume_tag_3',
        'customer_type',
        'realtime_scada_energy_tag_2',
        'meter_id',
        'quality_point_scada_c2',
        'realtime_scada_volume_tag_4',
        'gmdr_client_main_6',
        'quality_point_scada_n2',
        'gmdr_client_source',
        'gmdr_scada_main_2',
        'end_date',
        'monthly_client_id',
        'gmdr_scada_et_unit',
        'gmdr_client_main_4',
        'quality_point_scada_ic5',
        'gmdr_client_main_1',
        'quality_point_scada_ic4',
        'realtime_scada_energy_tag_5',
        'gmdr_client_back_1',
        'realtime_scada_energy_tag_4',
        'gmdr_client_svt_unit',
        'gmdr_scada_back_6',
        'quality_point_scada_nc5',
        'meter_name',
        'quality_point_scada_sg',
        'start_date',
        'gmdr_scada_svt_unit',
        'quality_point_scada_c6'
    ]; // fixed columns billing

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
            msg.put("jsonRsp", JSON.stringify({
                "Message": "No Data available."
            }));
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
