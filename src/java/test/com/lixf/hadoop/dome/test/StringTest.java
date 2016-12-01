package com.lixf.hadoop.dome.test;

import org.junit.Test;

public class StringTest {

	@Test
	public void test() {
		String test = "this is chinal,这是中国";
		String[] strs = test.split(" ");
		for (int i = 0; i < strs.length; i++) {
			System.out.println(strs[i]);
		}
	}

}
