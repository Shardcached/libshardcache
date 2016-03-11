package shardcache;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.istack.internal.Nullable;
import com.sun.tools.javac.code.Attribute;
import com.sun.xml.internal.ws.api.message.ExceptionHasMessage;

public class ShardcacheMessage {
   	static final byte HDR_GET        = (byte)0x01;
    static final byte HDR_SET        = (byte)0x02;
	static final byte HDR_DELETE     = (byte)0x03;
	static final byte HDR_EVICT      = (byte)0x04;
	static final byte HDR_GET_ASYNC  = (byte)0x05;
	static final byte HDR_GET_OFFSET = (byte)0x06;
	static final byte HDR_ADD        = (byte)0x07;
	static final byte HDR_EXISTS     = (byte)0x08;
	static final byte HDR_TOUCH      = (byte)0x09;

	static final byte HDR_NOOP       = (byte)0x90;
	static final byte HDR_RESPONSE   = (byte)0x99;

    static final byte HDR_CHECK      = (byte)0x31;
    static final byte HDR_STATS      = (byte)0x32;

    static final byte EOM = (byte)0x00; // end-of-message

    static final byte EORL = 0x00; // end-of-record low byte
    static final byte EORH = 0x00; // end-of-record high byte
    static final byte RSEP = (byte)0x80; // record-separator

	public enum Type {
		GET(HDR_GET),
		SET(HDR_SET),
        ADD(HDR_ADD),
		DELETE(HDR_DELETE),
		EVICT(HDR_EVICT),
		TOUCH(HDR_TOUCH),
		RESPONSE(HDR_RESPONSE),
		NOOP(HDR_NOOP),
        EXISTS(HDR_EXISTS),
        STATS(HDR_STATS),
        CHECK(HDR_CHECK);

		private byte byteVal;

		Type(byte b) {
			byteVal = b;
		}

        private static final Map<Byte, Type> lookup
                = new HashMap<Byte, Type>();

        static {
            for(Type t : EnumSet.allOf(Type.class))
                lookup.put(Byte.valueOf(t.getByte()), t);
        }

        public byte getByte() { return byteVal; }

        public static Type get(byte byteVal) {
            return lookup.get(Byte.valueOf(byteVal));
        }
	}

    public static class Builder {

        private ShardcacheMessage.Type type;
        private List<ShardcacheMessage.Record> records;
        private final byte[] magic = { 0x73, 0x68, 0x63, 0x02 };


        public Builder() {
            records = new ArrayList<Record>();
        }

        public void setMessageType(ShardcacheMessage.Type type) {
            this.type = type;
            status = Status.OK;
        }

        public void addRecord(byte[] data) {
            records.add(new Record(data));
        }

        enum Status {
            EMPTY,
            NEEDS_DATA,
            ERROR,
            OK
        };

        enum ParserStatus {
            EMPTY,
            MAGIC,
            HDR,
            RECORD,
            RSEP,
            DONE
        }

        private ParserStatus parserStatus = ParserStatus.EMPTY;

        Status status = Status.EMPTY;
        ByteArrayOutputStream inputAccumulator = new ByteArrayOutputStream();
        ByteArrayOutputStream recordAccumulator = new ByteArrayOutputStream();

        private int rsize;

        Status parse(byte[] data) throws Exception {
            return parse(data, data.length);
        }

        Status parse(byte[] data, int length) throws Exception {
            byte version;
            byte hdr;
            if (parserStatus == ParserStatus.DONE) {
                parserStatus = ParserStatus.EMPTY;
                status = Status.NEEDS_DATA;
                inputAccumulator.reset();
            }

            if (parserStatus == ParserStatus.EMPTY) {
                if (inputAccumulator.size() +  length < 4) {
                    inputAccumulator.write(data);
                    status = Status.NEEDS_DATA;
                    return status;
                }
                parserStatus = ParserStatus.MAGIC;
            }

            inputAccumulator.write(data, 0, length);
            byte[] pdata = inputAccumulator.toByteArray();
            int offset = 0;
            inputAccumulator.reset();

            if (parserStatus == ParserStatus.MAGIC) {
                if (pdata[0] == 0x73 && pdata[1] == 0x68 && pdata[2] == 0x63) {
                    version = pdata[3];
                    offset = 4;
                    parserStatus = ParserStatus.HDR;
                } else {
                    // BAD MAGIC;
                    parserStatus = ParserStatus.EMPTY;
                    status = Status.ERROR;
                    return status;
                }
            }
            if (parserStatus == ParserStatus.HDR) {
                if (pdata.length > offset) { // we need 1 byte
                    hdr = pdata[offset++];
                    type = Type.get(hdr);
                    parserStatus = ParserStatus.RECORD;
                    recordAccumulator.reset();
                    // TODO - check hdr
                } else {
                    // we consumed all the data
                    status = Status.NEEDS_DATA;
                    //inputAccumulator.write(pdata, offset, pdata.length - offset);
                    return status;
                }
            }

            while (parserStatus == ParserStatus.RECORD ||
                   parserStatus == ParserStatus.RSEP)
            {
                if (parserStatus == ParserStatus.RECORD) {
                    if (pdata.length - offset > 4) {
                        ByteBuffer byteBuffer = ByteBuffer.wrap(pdata, offset, 4);
                        int size = byteBuffer.getInt();
                        if (size == 0) { // End-Of-Record
                            offset += 4;
                            parserStatus = ParserStatus.RSEP;
                            addRecord(recordAccumulator.toByteArray());
                            recordAccumulator.reset();
                        } else if (pdata.length - offset >= size + 4) {
                            offset += 4;
                            recordAccumulator.write(pdata, offset, size);
                            offset += size;
                            parserStatus = ParserStatus.RSEP;
                        } else {
                            inputAccumulator.write(pdata, offset, pdata.length - offset);
                            status = Status.NEEDS_DATA;
                            return status;
                        }
                    }
                }

                if (parserStatus == ParserStatus.RSEP) {
                    if (pdata.length - offset > 0) {
                        byte rsep = pdata[offset++];
                        if (rsep == RSEP || rsep == EOM) {
                            Record record = new Record(recordAccumulator.toByteArray());
                            recordAccumulator.reset();
                            records.add(record);
                            if (rsep == EOM) {
                                parserStatus = ParserStatus.DONE;
                                status = Status.OK;
                            } else {
                                parserStatus = ParserStatus.RECORD;
                            }
                        } else {
                            status = Status.ERROR;
                            parserStatus = ParserStatus.EMPTY;
                            break;
                        }
                    } else {
                        // we consumed all the data
                        status = Status.NEEDS_DATA;
                        return status;
                    }
                }
            }
            return status;
        }

        @Nullable
        public ShardcacheMessage build() {
            ShardcacheMessage msg = null;
            if (status == Status.OK) {
                try {
                    msg = new ShardcacheMessage(type, records);
                } catch (Exception e) {
                    // TODO - report the error
                }
            }
            return msg;
        }


    }

    public static class Record {
        private final byte[] fullData;
        private final byte[] recordData;
        public Record(byte[] data) {
            fullData = data;

            recordData = new byte[4 + fullData.length];

            int offset = 0;
            int doffset = 0;

            ByteBuffer byteBuffer = ByteBuffer.allocate(4);
            byteBuffer.putInt(fullData.length);
            byte[] lenBytes = byteBuffer.array();

            recordData[offset++] = lenBytes[0];
            recordData[offset++] = lenBytes[1];
            recordData[offset++] = lenBytes[2];
            recordData[offset++] = lenBytes[3];
            for (int i = 0; i < fullData.length; i++)
                recordData[offset++] = data[doffset++];
            // EOR
            //recordData[offset++] = EOM;

        }

        byte[] getData() { return this.fullData; }

        byte[] getRecordData() { return this.recordData; }
    }

	boolean complete;
	final byte[] data;
	final byte version;
	final byte hdr;
	static final byte[] magic = { 0x73, 0x68, 0x63 };
	private final ShardcacheMessage.Record[] records;

	public ShardcacheMessage(ShardcacheMessage.Type type, List<ShardcacheMessage.Record> records) throws Exception {
        hdr = type.getByte();
        version = 2;

        ByteArrayOutputStream stream = new ByteArrayOutputStream();


        this.records = new ShardcacheMessage.Record[records.size()];
        int recordNum = 0;
        for (Record r: records) {
            if (recordNum > 0)
                stream.write(RSEP);
            this.records[recordNum++] = r;
            stream.write(r.recordData);
        }

        this.data = new byte[4 + 1 + stream.size() + 1];

        // magic
		data[0] = magic[0];
		data[1] = magic[1];
		data[2] = magic[2];
		data[3] = 0x02; // protocol version

		data[4] = hdr;

        System.arraycopy(stream.toByteArray(), 0, data, 5, stream.size());
        data[data.length-1] = EOM;
	}
	
	public int numRecords() { return records.length; }

    @Nullable
	public ShardcacheMessage.Record recordAtIndex(int index) {
        if (records.length <= index)
            return null;
        return records[index];
    }
}


