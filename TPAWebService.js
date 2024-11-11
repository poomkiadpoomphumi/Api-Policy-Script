with (new JavaImporter(com.vordel.trace, java.util, java.text.SimpleDateFormat)) {
    var col = [
        "nc4",
        "dewpoint",
        "hg",
        "nc5",
        "mapp",
        "ic5",
        "volume",
        "pressure",
        "registertimestamp",
        "energy",
        "wobbiexindex",
        "heatingvalue",
        "co2",
        "ic4",
        "c7",
        "c1",
        "o2",
        "c2",
        "c3",
        "s",
        "n2",
        "h2s",
        "c2p",
        "c6",
        "c8"
    ]; // fixed columns

    function convertNullToEmptyString(val) {
        return val == null ? "" : val.toString();
    }
    function formatDate(params) {
        if (params == null) return "";
        var dt = new Date(parseInt(params, 10));
        var dtFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dtFormat.format(dt);
    }
    function invoke(msg) {
        var records = {};
        var val = msg.get("tpa"); 
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
                    if(key === 'registertimestamp'){
                        record[key] = formatDate(item.get(key));
                    }else if(key === 'wobbiexindex'){
                            record[key] = null;
                    }else{
                        record[key] = convertNullToEmptyString(item.get(key)); // Convert null to empty string for each key
                    }
                }
                var meteringPointId = item.get('meteringpointid'); // Assuming 'meteringpointid' is the identifier for the records
                records[meteringPointId] = record; // Map the meteringPointId to the record
            }
    
            var output = { "RECORDS": records };
            msg.put("jsonRsp", JSON.stringify(output)); // Uncomment to set the output message
            return true;
        } catch (e) {
            Trace.error("Error in invoke function: " + e.toString());
            msg.put("error", JSON.stringify({ "Message": "An error occurred: " + e.toString() }));
            return false;
        }
    }    
}
