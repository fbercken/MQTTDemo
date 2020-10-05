package org.hpe.dnslog;

public class DNSLogEntry  {
	
	// See attribute details: http://gauss.ececs.uc.edu/Courses/c6055/pdf/bro_log_vars.pdf
	
	long ts;
	String uid;
	String origin_h;
	String origin_p;
	String resp_h;
	String resp_p;
	String proto;
	int port;
	String query;
	float qclass;
	String qclass_name;
	float qtype;
	String qtype_name;
	float rcode;
	String rcode_name;
	boolean OR;
	boolean AA;
	boolean TC;
	boolean RD;
	int Z;
	String[] Answers;
	int[] TLLs;
	boolean rejected;




	public DNSLogEntry() {
	}

	public long getTs() {
		return ts;
	}
	public void setTs(long ts) {
		this.ts = ts;
	}
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public String getOrigin_h() {
		return origin_h;
	}
	public void setOrigin_h(String origin_h) {
		this.origin_h = origin_h;
	}
	public String getOrigin_p() {
		return origin_p;
	}
	public void setOrigin_p(String origin_p) {
		this.origin_p = origin_p;
	}
	public String getResp_h() {
		return resp_h;
	}
	public void setResp_h(String resp_h) {
		this.resp_h = resp_h;
	}
	public String getResp_p() {
		return resp_p;
	}
	public void setResp_p(String resp_p) {
		this.resp_p = resp_p;
	}
	public String getProto() {
		return proto;
	}
	public void setProto(String proto) {
		this.proto = proto;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public float getQclass() {
		return qclass;
	}
	public void setQclass(float qclass) {
		this.qclass = qclass;
	}
	public String getQclass_name() {
		return qclass_name;
	}
	public void setQclass_name(String qclass_name) {
		this.qclass_name = qclass_name;
	}
	public float getQtype() {
		return qtype;
	}
	public void setQtype(float qtype) {
		this.qtype = qtype;
	}
	public String getQtype_name() {
		return qtype_name;
	}
	public void setQtype_name(String qtype_name) {
		this.qtype_name = qtype_name;
	}
	public float getRcode() {
		return rcode;
	}
	public void setRcode(float rcode) {
		this.rcode = rcode;
	}
	public String getRcode_name() {
		return rcode_name;
	}
	public void setRcode_name(String rcode_name) {
		this.rcode_name = rcode_name;
	}
	public boolean isOR() {
		return OR;
	}
	public void setOR(boolean oR) {
		OR = oR;
	}
	public boolean isAA() {
		return AA;
	}
	public void setAA(boolean aA) {
		AA = aA;
	}
	public boolean isTC() {
		return TC;
	}
	public void setTC(boolean tC) {
		TC = tC;
	}
	public boolean isRD() {
		return RD;
	}
	public void setRD(boolean rD) {
		RD = rD;
	}
	public int getZ() {
		return Z;
	}
	public void setZ(int z) {
		Z = z;
	}
	public String[] getAnswers() {
		return Answers;
	}
	public void setAnswers(String[] answers) {
		Answers = answers;
	}
	public int[] getTLLs() {
		return TLLs;
	}
	public void setTLLs(int[] tLLs) {
		TLLs = tLLs;
	}
	public boolean isRejected() {
		return rejected;
	}
	public void setRejected(boolean rejected) {
		this.rejected = rejected;
	}

}


