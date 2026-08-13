use crate::*;

pub use decoder::ExecutionReportDecoder;
pub use encoder::ExecutionReportEncoder;

pub use crate::SBE_SCHEMA_ID;
pub use crate::SBE_SCHEMA_VERSION;
pub use crate::SBE_SEMANTIC_VERSION;

pub const SBE_BLOCK_LENGTH: u16 = 47;
pub const SBE_TEMPLATE_ID: u16 = 150;

pub mod encoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Debug, Default)]
    pub struct ExecutionReportEncoder<'a> {
        buf: WriteBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
    }

    impl<'a> Writer<'a> for ExecutionReportEncoder<'a> {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            &mut self.buf
        }
    }

    impl<'a> Encoder<'a> for ExecutionReportEncoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> ExecutionReportEncoder<'a> {
        pub fn wrap(mut self, buf: WriteBuf<'a>, offset: usize) -> Self {
            let limit = offset + SBE_BLOCK_LENGTH as usize;
            self.buf = buf;
            self.initial_offset = offset;
            self.offset = offset;
            self.limit = limit;
            self
        }

        #[inline]
        pub const fn encoded_length(&self) -> usize {
            self.limit - self.offset
        }

        #[inline]
        pub fn header(self, offset: usize) -> MessageHeaderEncoder<Self> {
            let mut header = MessageHeaderEncoder::default().wrap(self, offset);
            header.block_length(SBE_BLOCK_LENGTH);
            header.template_id(SBE_TEMPLATE_ID);
            header.schema_id(SBE_SCHEMA_ID);
            header.version(SBE_SCHEMA_VERSION);
            header
        }

        /// primitive field 'engineSeq'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 0
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn engine_seq(&mut self, value: u64) -> &mut Self {
            let offset = self.offset;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'clientOrderId'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 8
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn client_order_id(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 8;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'accountId'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 16
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn account_id(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 16;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// REQUIRED enum
        #[inline]
        pub fn exec_type(&mut self, value: exec_type::ExecType) -> &mut Self {
            let offset = self.offset + 20;
            self.get_buf_mut().put_u8_at(offset, value as u8);
            self
        }

        /// REQUIRED enum
        #[inline]
        pub fn ord_status(&mut self, value: ord_status::OrdStatus) -> &mut Self {
            let offset = self.offset + 21;
            self.get_buf_mut().put_u8_at(offset, value as u8);
            self
        }

        /// REQUIRED enum
        #[inline]
        pub fn reject_reason(&mut self, value: reject_reason::RejectReason) -> &mut Self {
            let offset = self.offset + 22;
            self.get_buf_mut().put_u8_at(offset, value as u8);
            self
        }

        /// primitive field 'fillPrice'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 23
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn fill_price(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 23;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'fillQty'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 31
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn fill_qty(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 31;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'remainingQty'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 39
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn remaining_qty(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 39;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }
    }
} // end encoder

pub mod decoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Clone, Copy, Debug, Default)]
    pub struct ExecutionReportDecoder<'a> {
        buf: ReadBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
        pub acting_block_length: u16,
        pub acting_version: u16,
    }

    impl ActingVersion for ExecutionReportDecoder<'_> {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.acting_version
        }
    }

    impl<'a> Reader<'a> for ExecutionReportDecoder<'a> {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            &self.buf
        }
    }

    impl<'a> Decoder<'a> for ExecutionReportDecoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> ExecutionReportDecoder<'a> {
        pub fn wrap(mut self, buf: ReadBuf<'a>, offset: usize, acting_block_length: u16, acting_version: u16) -> Self {
            let limit = offset + acting_block_length as usize;
            self.buf = buf;
            self.initial_offset = offset;
            self.offset = offset;
            self.limit = limit;
            self.acting_block_length = acting_block_length;
            self.acting_version = acting_version;
            self
        }

        #[inline]
        pub const fn encoded_length(&self) -> usize {
            self.limit - self.offset
        }

        #[inline]
        pub fn header(self, mut header: MessageHeaderDecoder<ReadBuf<'a>>, offset: usize) -> Self {
            debug_assert_eq!(SBE_TEMPLATE_ID, header.template_id());
            let acting_block_length = header.block_length();
            let acting_version = header.version();

            self.wrap(
                header.parent().unwrap(),
                offset + message_header_codec::ENCODED_LENGTH,
                acting_block_length,
                acting_version,
            )
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn engine_seq(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn client_order_id(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 8)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn account_id(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 16)
        }

        /// REQUIRED enum
        #[inline]
        pub fn exec_type(&self) -> exec_type::ExecType {
            self.get_buf().get_u8_at(self.offset + 20).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn ord_status(&self) -> ord_status::OrdStatus {
            self.get_buf().get_u8_at(self.offset + 21).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn reject_reason(&self) -> reject_reason::RejectReason {
            self.get_buf().get_u8_at(self.offset + 22).into()
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn fill_price(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 23)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn fill_qty(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 31)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn remaining_qty(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 39)
        }
    }
} // end decoder
