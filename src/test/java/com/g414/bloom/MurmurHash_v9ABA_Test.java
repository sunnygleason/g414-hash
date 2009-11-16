package com.g414.bloom;

import com.g414.hash.LongHash;
import com.g414.hash.MurmurHash_v9ABA;

public class MurmurHash_v9ABA_Test extends BloomFilterTestBase {
	@Override
	public LongHash getHash() {
		return new MurmurHash_v9ABA();
	}
}
