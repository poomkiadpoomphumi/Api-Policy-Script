var imp = new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat);

with (imp) {
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
        var col = [
            'source_name', 'update_timestamp', 'customer_type', 'gasday', 'abnormal_case',
            'meter_id', 'run', 'tag', 'source', 'description', 'meter_name', 'error_type',
            'cur_time', 'percentage'
        ]; // fixed columns
        try {
            for (var i = 0; i < val.length; i++) {
                var record = {};
                var item = val.get(i);
                for (var j = 0; j < col.length; j++) {
                    var key = col[j];
                    var source = parseInt(item.get('source'), 10);
                    var run = parseInt(item.get('run'), 10);
                    if (key === 'source') {
                        record[key] = convertNullToEmptyString(item.get(key));
                        record['source_name'] = _upadteSourceName(item, source);
                    } else if (key === 'tag') {
                        record[key] = _updateTagScada(item, source, run);
                    } else if (key === 'percentage' || key === 'cur_time') {
                        var ab_case = parseInt(item.get('abnormal_case'), 10);
                        var err_type = parseInt(item.get('error_type'), 10);
                        record['cur_time'] = _createNewCol(ab_case, item, err_type, true);
                        record['percentage'] = _createNewCol(ab_case, item, err_type, false);
                    } else {
                        record[key] = convertNullToEmptyString(item.get(key));
                    }
                }
                records.push(convertKeysToUppercase(record));
            }
            msg.put("jsonRsp", JSON.stringify({ "records": records }));
            return true;
        } catch (e) {
            msg.put("jsonRsp", JSON.stringify({ "Message": "An error occurred: " + e.toString() }));
            return false;
        }
    }
    function _createNewCol(ab_case, item, err_type, col) {
        var meter_id = parseInt(item.get('meter_id'), 10);
        if (ab_case === 13) {
            if (col === true) {//cur_time column
                var matches1 = item.get('description').match(/(\d{2}:\d{2})/);
                if (matches1 && matches1[1]) { return matches1[1]; } else { return "" }
            }
            if (col === false) {//percentage column
                var matches2 = item.get('description').match(/(\d{1,3})%/);
                if (matches2 && matches2[1]) { return matches2[1]; } else { return "" }
            }
        } else if (err_type === 1 && ab_case === 5 && meter_id === meter_id) {
            var MissHour = item.get('description').match(/(\d{2}:\d{2})/g); // Use 'g' flag to get all matches
            var pattern = /พบค่าไม่เข้าช่วงเวลา \d{2}:\d{2} ถึง \d{2}:\d{2}/;
            var matcher = item.get('description').match(pattern);
            if (MissHour && MissHour.length >= 3) { return MissHour[2]; } 
            if (matcher[0]) { return matcher[0] }
        } else {
            return "";
        }

    }
    function _upadteSourceName(item, source) {
        if (item.get('gmdr_scada_main_1') !== '' &&
            item.get('gmdr_client_main_1') !== '' &&
            source === 3 && source === 4) {
            return 'GMDR SCADA + GMDR CLIENT';
        }
        switch (source) {
            case 1: return 'MONTHLY BILLING';
            case 2: return 'DAILY BILLING';
            case 3: return 'GMDR SCADA';
            case 4: return 'GMDR CLIENT';
            case 5: return 'ARCH SCADA';
            case 6: return 'REALTIME SCADA';
            default: return source;
        }

    }

    function _updateTagScada(item, source, run) {
        if (parseInt(item.get('abnormal_case'), 10) === 13) {
            return item.get('realtime_scada_volume_tag_1');
        }
        switch (source) {
            case 1: return item.get('monthly_client_id');
            case 2: return item.get('daily_client_id');
            case 3: switch (run) {
                case 1: return item.get('gmdr_scada_main_1');
                case 2: return item.get('gmdr_scada_main_2');
                case 3: return item.get('gmdr_scada_main_3');
                case 4: return item.get('gmdr_scada_main_4');
                case 5: return item.get('gmdr_scada_main_5');
                case 6: return item.get('gmdr_scada_main_6');
                case 7: return item.get('gmdr_scada_back_1');
                case 8: return item.get('gmdr_scada_back_2');
                case 9: return item.get('gmdr_scada_back_3');
                case 10: return item.get('gmdr_scada_back_4');
                case 11: return item.get('gmdr_scada_back_5');
                case 12: return item.get('gmdr_scada_back_6');
            }
            case 4: switch (run) {
                case 1: return item.get('gmdr_client_main_1');
                case 2: return item.get('gmdr_client_main_2');
                case 3: return item.get('gmdr_client_main_3');
                case 4: return item.get('gmdr_client_main_4');
                case 5: return item.get('gmdr_client_main_5');
                case 6: return item.get('gmdr_client_main_6');
                case 7: return item.get('gmdr_client_back_1');
                case 8: return item.get('gmdr_client_back_2');
                case 9: return item.get('gmdr_client_back_3');
                case 10: return item.get('gmdr_client_back_4');
                case 12: return item.get('gmdr_client_back_5');
                case 13: return item.get('gmdr_client_back_6');
            }
            case 5: switch (run) {
                case 1: return item.get('arch_scada_volume_tag_1');
                case 2: return item.get('arch_scada_volume_tag_2');
                case 3: return item.get('arch_scada_volume_tag_3');
                case 4: return item.get('arch_scada_volume_tag_4');
                case 5: return item.get('arch_scada_volume_tag_5');
                case 6: return item.get('arch_scada_volume_tag_6');
                case 7: return item.get('arch_scada_energy_tag_1');
                case 8: return item.get('arch_scada_energy_tag_2');
                case 9: return item.get('arch_scada_energy_tag_3');
                case 10: return item.get('arch_scada_energy_tag_4');
                case 11: return item.get('arch_scada_energy_tag_5');
                case 12: return item.get('arch_scada_energy_tag_6');
            }
            case 6: switch (run) {
                case 1: return item.get('realtime_scada_volume_tag_1');
                case 2: return item.get('realtime_scada_volume_tag_2');
                case 3: return item.get('realtime_scada_volume_tag_3');
                case 4: return item.get('realtime_scada_volume_tag_4');
                case 5: return item.get('realtime_scada_volume_tag_5');
                case 6: return item.get('realtime_scada_volume_tag_6');
                case 7: return item.get('realtime_scada_energy_tag_1');
                case 8: return item.get('realtime_scada_energy_tag_2');
                case 9: return item.get('realtime_scada_energy_tag_3');
                case 10: return item.get('realtime_scada_energy_tag_4');
                case 11: return item.get('realtime_scada_energy_tag_5');
                case 12: return item.get('realtime_scada_energy_tag_6');
            }
            default: return "";
        }
    }
}
