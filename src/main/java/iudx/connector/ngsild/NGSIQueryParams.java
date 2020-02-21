package iudx.connector.ngsild;

import java.net.URI;
import java.util.List;

public class NGSIQueryParams {

	private List<URI> id;
	private List<String> type;
	private List<String> attrs;
	private String idPattern;
	private String q;
	private String geoRel;
	private String geometry;
	private String coordinates;
	private String geoProperty;

	public List<URI> getId() {
		return id;
	}

	public void setId(List<URI> id) {
		this.id = id;
	}

	public List<String> getType() {
		return type;
	}

	public void setType(List<String> type) {
		this.type = type;
	}

	public List<String> getAttrs() {
		return attrs;
	}

	public void setAttrs(List<String> attrs) {
		this.attrs = attrs;
	}

	public String getIdPattern() {
		return idPattern;
	}

	public void setIdPattern(String idPattern) {
		this.idPattern = idPattern;
	}

	public String getQ() {
		return q;
	}

	public void setQ(String q) {
		this.q = q;
	}

	public String getGeoRel() {
		return geoRel;
	}

	public void setGeoRel(String geoRel) {
		this.geoRel = geoRel;
	}

	public String getGeometry() {
		return geometry;
	}

	public void setGeometry(String geometry) {
		this.geometry = geometry;
	}

	public String getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(String coordinates) {
		this.coordinates = coordinates;
	}

	public String getGeoProperty() {
		return geoProperty;
	}

	public void setGeoProperty(String geoProperty) {
		this.geoProperty = geoProperty;
	}

}
