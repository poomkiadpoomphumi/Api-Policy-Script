import groovy.json.JsonOutput

def invoke(msg) {
    try {
            def records = [] 
            def array = []
            def count = 0
            def tagGmdrScada = ''
            def tagGmdrClient = ''
            def getColumn = [
                'update_timestamp', 'customer_type', 'gasday', 'abnormal_case', 'meter_id',
                'run', 'tag', 'source', 'description', 'meter_name', 'error_type'
            ]
            def obj = msg.get("obj")
            obj.keySet().each { columns -> array << columns }
            if (!array.isEmpty() && obj != null) {
                while (true) {
                    def row = [:] 
                    array.each { key ->
                        if (key in getColumn) {
                            row[key.toUpperCase()] = obj[key]["values"][count]
                        }
                        updateTags(obj, count, tagGmdrScada, tagGmdrClient)
                        updateSourceName(obj, count, row, tagGmdrScada, tagGmdrClient) 
                        updateTag(obj, count, row)
                        updateCurrentTime(obj, count, row)
                    }
                    records << row 
                    count++
                    if (obj[array[0]]["values"].size() == count) {
                        break
                    }
                }
            } else {
                msg.put("error", "This information is not available in the database.")
                return false
            }

            def response = [:]
            response["records"] = records
            
            def jsonBeauty = new String(JsonOutput.prettyPrint(JsonOutput.toJson(response)).getBytes("UTF-8"))
            msg.put("jsonRsp", jsonBeauty)
            return true
        } catch (Exception e) {
            msg.put("error", e.toString())
            return false
        }
}


def updateTags(obj, count, tagGmdrScada, tagGmdrClient) {
    if (obj["source"]["values"][count] == '3' && obj["run"]["values"][count] == '1') {
        tagGmdrScada = obj["gmdr_scada_main_1"]["values"][count]
    }
    if (obj["source"]["values"][count] == '4' && obj["run"]["values"][count] == '1') {
        tagGmdrClient = obj["gmdr_client_main_1"]["values"][count]
    }
}

def updateSourceName(obj, count, row, tagGmdrScada, tagGmdrClient) { 
    if (tagGmdrScada != null && tagGmdrClient != null) {
        row["SOURCE_NAME"] = 'GMDR SCADA + GMDR CLIENT'
    } 
    if (tagGmdrScada != null) {
        row["SOURCE_NAME"] = 'GMDR SCADA'
    } 
    if (tagGmdrClient != null) {
        row["SOURCE_NAME"] = 'GMDR CLIENT'
    } 
    if (obj["source"]["values"][count] == '5') {
        row["SOURCE_NAME"] = 'ARCH SCADA'
    } 
    if (obj["source"]["values"][count] == '6') {
        row["SOURCE_NAME"] = 'REALTIME SCADA'
    }
}

def updateTag(obj, count, row) {
    def source = obj["source"]["values"][count]
    def run = obj["run"]["values"][count]

    if (source == '1') {
        row["TAG"] = obj["monthly_client_id"]["values"][count]
        row["SOURCE_NAME"] = 'MONTHLY BILLING'
    } else if (source == '2') {
        row["TAG"] = obj["daily_client_id"]["values"][count]
        row["SOURCE_NAME"] = 'DAILY BILLING'
    } else if (source == '3') {
        if (run == '1') {
            row["TAG"] = obj["gmdr_scada_main_1"]["values"][count]
        } else if (run == '2') {
            row["TAG"] = obj["gmdr_scada_main_2"]["values"][count]
        } else if (run == '3') {
            row["TAG"] = obj["gmdr_scada_main_3"]["values"][count]
        } else if (run == '4') {
            row["TAG"] = obj["gmdr_scada_main_4"]["values"][count]
        } else if (run == '5') {
            row["TAG"] = obj["gmdr_scada_main_5"]["values"][count]
        } else if (run == '6') {
            row["TAG"] = obj["gmdr_scada_main_6"]["values"][count]
        } else if (run == '7') {
            row["TAG"] = obj["gmdr_scada_back_1"]["values"][count]
        } else if (run == '8') {
            row["TAG"] = obj["gmdr_scada_back_2"]["values"][count]
        } else if (run == '9') {
            row["TAG"] = obj["gmdr_scada_back_3"]["values"][count]
        } else if (run == '10') {
            row["TAG"] = obj["gmdr_scada_back_4"]["values"][count]
        } else if (run == '11') {
            row["TAG"] = obj["gmdr_scada_back_5"]["values"][count]
        } else if (run == '12') {
            row["TAG"] = obj["gmdr_scada_back_6"]["values"][count]
        }
    } else if (source == '4') {
        if (run == '1') {
            row["TAG"] = obj["gmdr_client_main_1"]["values"][count]
        } else if (run == '2') {
            row["TAG"] = obj["gmdr_client_main_2"]["values"][count]
        } else if (run == '3') {
            row["TAG"] = obj["gmdr_client_main_3"]["values"][count]
        } else if (run == '4') {
            row["TAG"] = obj["gmdr_client_main_4"]["values"][count]
        } else if (run == '5') {
            row["TAG"] = obj["gmdr_client_main_5"]["values"][count]
        } else if (run == '6') {
            row["TAG"] = obj["gmdr_client_main_6"]["values"][count]
        } else if (run == '7') {
            row["TAG"] = obj["gmdr_client_back_1"]["values"][count]
        } else if (run == '8') {
            row["TAG"] = obj["gmdr_client_back_2"]["values"][count]
        } else if (run == '9') {
            row["TAG"] = obj["gmdr_client_back_3"]["values"][count]
        } else if (run == '10') {
            row["TAG"] = obj["gmdr_client_back_4"]["values"][count]
        } else if (run == '11') {
            row["TAG"] = obj["gmdr_client_back_5"]["values"][count]
        } else if (run == '12') {
            row["TAG"] = obj["gmdr_client_back_6"]["values"][count]
        }
    } else if (source == '5') {
        if (run == '1') {
            row["TAG"] = obj["arch_scada_volume_tag_1"]["values"][count]
        } else if (run == '2') {
            row["TAG"] = obj["arch_scada_volume_tag_2"]["values"][count]
        } else if (run == '3') {
            row["TAG"] = obj["arch_scada_volume_tag_3"]["values"][count]
        } else if (run == '4') {
            row["TAG"] = obj["arch_scada_volume_tag_4"]["values"][count]
        } else if (run == '5') {
            row["TAG"] = obj["arch_scada_volume_tag_5"]["values"][count]
        } else if (run == '6') {
            row["TAG"] = obj["arch_scada_volume_tag_6"]["values"][count]
        } else if (run == '7') {
            row["TAG"] = obj["arch_scada_energy_tag_1"]["values"][count]
        } else if (run == '8') {
            row["TAG"] = obj["arch_scada_energy_tag_2"]["values"][count]
        } else if (run == '9') {
            row["TAG"] = obj["arch_scada_energy_tag_3"]["values"][count]
        } else if (run == '10') {
            row["TAG"] = obj["arch_scada_energy_tag_4"]["values"][count]
        } else if (run == '11') {
            row["TAG"] = obj["arch_scada_energy_tag_5"]["values"][count]
        } else if (run == '12') {
            row["TAG"] = obj["arch_scada_energy_tag_6"]["values"][count]
        }
    } else if (source == '6') {
        if (run == '1') {
            row["TAG"] = obj["realtime_scada_volume_tag_1"]["values"][count]
        } else if (run == '2') {
            row["TAG"] = obj["realtime_scada_volume_tag_2"]["values"][count]
        } else if (run == '3') {
            row["TAG"] = obj["realtime_scada_volume_tag_3"]["values"][count]
        } else if (run == '4') {
            row["TAG"] = obj["realtime_scada_volume_tag_4"]["values"][count]
        } else if (run == '5') {
            row["TAG"] = obj["realtime_scada_volume_tag_5"]["values"][count]
        } else if (run == '6') {
            row["TAG"] = obj["realtime_scada_volume_tag_6"]["values"][count]
        } else if (run == '7') {
            row["TAG"] = obj["realtime_scada_energy_tag_1"]["values"][count]
        } else if (run == '8') {
            row["TAG"] = obj["realtime_scada_energy_tag_2"]["values"][count]
        } else if (run == '9') {
            row["TAG"] = obj["realtime_scada_energy_tag_3"]["values"][count]
        } else if (run == '10') {
            row["TAG"] = obj["realtime_scada_energy_tag_4"]["values"][count]
        } else if (run == '11') {
            row["TAG"] = obj["realtime_scada_energy_tag_5"]["values"][count]
        } else if (run == '12') {
            row["TAG"] = obj["realtime_scada_energy_tag_6"]["values"][count]
        }
    } else {
        row["TAG"] = null
    }
}


def updateCurrentTime(obj, count, row) {
    if (obj["abnormal_case"]["values"][count] == null) {
        obj["abnormal_case"]["values"][count] = null
    }else if (obj["abnormal_case"]["values"][count] == '13') {
        row["TAG"] = obj["realtime_scada_volume_tag_1"]["values"][count]
        def matches = (obj["description"]["values"][count] =~ /(\d{2}:\d{2})/)
        row["CUR_TIME"] = matches[1][0]
        def matcherPercen = (obj["description"]["values"][count] =~ /(\d{1,3})%/)
        if (matcherPercen.find()) {
            row["PERCENTAGE"] = matcherPercen.group(1)
/*             if(matcherPercen.group(1) == '150'){
                row["PERCENTAGE"] = 1
            }else {
                row["PERCENTAGE"] = 0
            }
            if(matcherPercen.group(1) == '10'){
                row["PERCENTAGE"] = 1
            }else{
                row["PERCENTAGE"] = 0
            } */
        }
    }else if(obj["error_type"]["values"][count] == '1' && 
       obj["abnormal_case"]["values"][count] == '5' &&
       obj["meter_id"]["values"][count] == obj["meter_id"]["values"][count]){
       def MissHour = (obj["description"]["values"][count] =~ /(\d{2}:\d{2})/)
       if (MissHour.size() >= 3) { //get hour missing time to new column send to power bi
            row["CUR_TIME"] = MissHour[2][0] 
       } 
       def pattern = /พบค่าไม่เข้าช่วงเวลา \d{2}:\d{2} ถึง \d{2}:\d{2}/ 
       def matcher = (obj["description"]["values"][count] =~ pattern)
       if (matcher.find()) { //No get hour missing time to new column send to power bi
            def extractedString = matcher.group()
            obj["description"]["values"][count] = extractedString
       } else {
            return false
       }
    }
}