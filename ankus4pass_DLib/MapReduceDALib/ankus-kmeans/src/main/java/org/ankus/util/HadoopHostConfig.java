package org.ankus.util;

import com.fasterxml.jackson.annotation.JsonProperty;
public class HadoopHostConfig {
	
	@JsonProperty("fs_default_name")
	private String fs_default_name;

	public String getFs_default_name() {
		return fs_default_name;
	}

	public void setFs_default_name(String fs_default_name) {
		this.fs_default_name = fs_default_name;
	}
}
