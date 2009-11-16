package com.g414.bloom;

import com.g414.hash.LongHash;
import com.g414.hash.Sha1PrngHash_v9ABA;

public class Sha1PrngHash_v9ABA_Test extends BloomFilterTestBase {
	@Override
	public LongHash getHash() {
		return new Sha1PrngHash_v9ABA();
	}
}
