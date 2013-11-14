package swift.test.microbenchmark;


public class SerializerTester {

    /**
     * @param args
     */
    public static void main(String[] args) {
    	/*
    	 * Needs to be converted to Kryo v2
    	 * 
        Kryo kryo = new Kryo();
        VersionVectorWithExceptions vv = new VersionVectorWithExceptions();

        kryo.register(Map.class, new MapSerializer(kryo));
        kryo.register(HashMap.class, new MapSerializer(kryo));
        kryo.register(ArrayList.class, new CollectionSerializer(kryo));
        kryo.register(LinkedHashMap.class, new MapSerializer(kryo));
        kryo.register(VersionVectorWithExceptions.class);
        
        ByteBuffer bb = ByteBuffer.allocate(65536);
        kryo.writeObject(bb, "teste");
        */

    }

}
