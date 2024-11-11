import groovy.json.JsonOutput
import groovy.json.StringEscapeUtils
import groovy.json.*
import java.util.LinkedHashMap

class tpasub{
	String C1
    String C2
	String C2P
    String C3
	String C6
	String C7
	String C8
	String CO2
	String DEWPOINT
	String ENERGY
	String H2S
	String HEATINGVALUE
	String HG
	String IC4
	String IC5
	String N2
	String NC4
	String NC5
	String O2
	String REGISTERTIMESTAMP
	String S
	String VOLUME
	String WOBBIEXINDEX
	String MAPP
	String PRESSURE

    def tpasub(String C1,String C2,String C2P,String C3,String C6,String C7,String C8,String CO2,String DEWPOINT,String ENERGY,String H2S,String HEATINGVALUE,String HG,String IC4,String IC5,String N2,String NC4,String NC5,String O2,String REGISTERTIMESTAMP,String S,String VOLUME,String WOBBIEXINDEX,String MAPP,String PRESSURE){
		this.C1=C1
		this.C2=C2
		this.C2P=C2P
		this.C3=C3
		this.C6=C6
		this.C7=C7
		this.C8=C8
		this.CO2=CO2
		this.DEWPOINT=DEWPOINT
		this.ENERGY=ENERGY
		this.H2S=H2S
		this.HEATINGVALUE=HEATINGVALUE
		this.HG=HG
		this.IC4=IC4
		this.IC5=IC5
		this.N2=N2
		this.NC4=NC4
		this.NC5=NC5
		this.O2=O2
		this.REGISTERTIMESTAMP=REGISTERTIMESTAMP
		this.S=S
		this.VOLUME=VOLUME
		this.WOBBIEXINDEX=WOBBIEXINDEX
		this.MAPP=MAPP
		this.PRESSURE=PRESSURE
		}
}

class tpa{
	String meteringpointid
	Object tpavalue
	def tpa(String meteringpointid,tpasub tpavalue){
		this.meteringpointid=meteringpointid
		this.tpavalue=tpavalue
	}	
}

class tpas{
	Map<String,tpasub> records=[:]
	
	def tpas(){
	}
	
	def addtpa(LinkedHashMap<String,tpasub> t){
		this.records<<t
	}
	def leftShift(LinkedHashMap<String,tpasub> t){
		addtpa(t)
		this
	}
}

def invoke(msg) 
{ 
	try{
	def dbcount = msg.get("db.result.count") as Integer;
	def count =1;
	def tpas =[];
	def dateT;
	
	tpas = new tpas();
	msg.put("errorTrace","0");
	if (dbcount>1){
		while (count<=dbcount){
				def meteringpointid= msg.get("tpa.meteringpointid."+count);
				def c1= msg.get("tpa.c1."+count);
				def c2= msg.get("tpa.c2."+count);
				def c2p= msg.get("tpa.c2p."+count);
				def c3= msg.get("tpa.c3."+count);
				def c6= msg.get("tpa.c6."+count);
				def c7= msg.get("tpa.c7."+count);
				def c8= msg.get("tpa.c8."+count);
				def co2= msg.get("tpa.co2."+count);
				def dewpoint= msg.get("tpa.dewpoint."+count);
				def energy= msg.get("tpa.energy."+count);
				def h2s= msg.get("tpa.h2s."+count);
				def heatingvalue= msg.get("tpa.heatingvalue."+count);
				def hg= msg.get("tpa.hg."+count);
				def ic4= msg.get("tpa.ic4."+count);
				def ic5= msg.get("tpa.ic5."+count);
				def n2= msg.get("tpa.n2."+count);
				def nc4= msg.get("tpa.nc4."+count);
				def nc5= msg.get("tpa.nc5."+count);
				def o2= msg.get("tpa.o2."+count);
				def s= msg.get("tpa.s."+count);
				def volume= msg.get("tpa.volume."+count);
				def wobbiexindex= msg.get("tpa.wobbiexindex."+count);
				def mapp= msg.get("tpa.mapp."+count);
				def pressure= msg.get("tpa.pressure."+count);
				msg.put("errorTrace","1");
				if(msg.get("tpa.registertimestamp."+count)!=""){
				def registertimestampd = new Date(Long.parseLong(msg.get("tpa.registertimestamp."+count)));
				registertimestamp=registertimestampd.format("yyyy-MM-dd HH:mm:ss");
				}
				def tpasub = new tpasub(c1,c2,c2p,c3,c6,c7,c8,co2,dewpoint,energy,h2s,heatingvalue,hg,ic4,ic5,n2,nc4,nc5,o2,registertimestamp,s,volume,wobbiexindex,mapp,pressure);
				def map=[:]
				map.put(meteringpointid,tpasub);
				def tpa =map;
				tpas << tpa;
				msg.put("count",count);
				count++;
			}
		}
	else if(dbcount==1){
				def meteringpointid= msg.get("tpa.meteringpointid");
				def c1= msg.get("tpa.c1");
				def c2= msg.get("tpa.c2");
				def c2p= msg.get("tpa.c2p");
				def c3= msg.get("tpa.c3");
				def c6= msg.get("tpa.c6");
				def c7= msg.get("tpa.c7");
				def c8= msg.get("tpa.c8");
				def co2= msg.get("tpa.co2");
				def dewpoint= msg.get("tpa.dewpoint");
				def energy= msg.get("tpa.energy");
				def h2s= msg.get("tpa.h2s");
				def heatingvalue= msg.get("tpa.heatingvalue");
				def hg= msg.get("tpa.hg");
				def ic4= msg.get("tpa.ic4");
				def ic5= msg.get("tpa.ic5");
				def n2= msg.get("tpa.n2");
				def nc4= msg.get("tpa.nc4");
				def nc5= msg.get("tpa.nc5");
				def o2= msg.get("tpa.o2");
				def s= msg.get("tpa.s");
				def volume= msg.get("tpa.volume");
				def wobbiexindex= msg.get("tpa.wobbiexindex");
				def mapp= msg.get("tpa.mapp");
				def pressure= msg.get("tpa.pressure");
				msg.put("errorTrace","1");
				if(msg.get("tpa.registertimestamp")!=""){
				def registertimestampd = new Date(Long.parseLong(msg.get("tpa.registertimestamp")));
				registertimestamp=registertimestampd.format("yyyy-MM-dd HH:mm:ss");
				}
				def tpasub = new tpasub(c1,c2,c2p,c3,c6,c7,c8,co2,dewpoint,energy,h2s,heatingvalue,hg,ic4,ic5,n2,nc4,nc5,o2,registertimestamp,s,volume,wobbiexindex,mapp,pressure);
				def tpa = new LinkedHashMap(meteringpointid,tpasub);
				tpas << tpa;
				}
	else{
			msg.put("jsonRsp","{\"Message\":\"No Data\"}");
		}
	if(dbcount>0){
		def json_str = new String(JsonOutput.toJson(tpas).getBytes("UTF-8"),"UTF-8");
		json_str = StringEscapeUtils.unescapeJava(json_str);
		json_str = new String(json_str.getBytes(),"UTF-8");
		
		JsonBuilder builder = new JsonBuilder(json_str);
		
		def json_beauty = JsonOutput.prettyPrint(json_str);

		println(json_beauty);
		json_beauty = StringEscapeUtils.unescapeJava(json_beauty );
		json_beauty = new String(json_beauty .getBytes(),"UTF-8");
		msg.put("jsonRsp",json_beauty );
	}
        return true;
	}catch(Exception e){
	msg.put("error",e.toString());
	return false;
}
}