use crate::*;

pub use decoder::CancelReplaceDecoder;
pub use encoder::CancelReplaceEncoder;

pub use crate::SBE_SCHEMA_ID;
pub use crate::SBE_SCHEMA_VERSION;
pub use crate::SBE_SEMANTIC_VERSION;

pub const SBE_BLOCK_LENGTH: u16 = 44;
pub const SBE_TEMPLATE_ID: u16 = 102;

pub mod encoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Debug, Default)]
    pub struct CancelReplaceEncoder<'a> {
        buf: WriteBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
    }

    impl<'a> Writer<'a> for CancelReplaceEncoder<'a> {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            &mut self.buf
        }
    }

    impl<'a> Encoder<'a> for CancelReplaceEncoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> CancelReplaceEncoder<'a> {
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

        /// primitive field 'clientOrderId'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 0
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn client_order_id(&mut self, value: u64) -> &mut Self {
            let offset = self.offset;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'accountId'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 8
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn account_id(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 8;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'origClientOrderId'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 12
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn orig_client_order_id(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 12;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'clientTs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 20
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn client_ts(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 20;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'price'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 28
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn price(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 28;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'qty'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 36
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn qty(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 36;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }
    }
} // end encoder

pub mod decoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Clone, Copy, Debug, Default)]
    pub struct CancelReplaceDecoder<'a> {
        buf: ReadBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
        pub acting_block_length: u16,
        pub acting_version: u16,
    }

    impl ActingVersion for CancelReplaceDecoder<'_> {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.acting_version
        }
    }

    impl<'a> Reader<'a> for CancelReplaceDecoder<'a> {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            &self.buf
        }
    }

    impl<'a> Decoder<'a> for CancelReplaceDecoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> CancelReplaceDecoder<'a> {
        pub fn wrap(
            mut self,
            buf: ReadBuf<'a>,
            offset: usize,
            acting_block_length: u16,
            acting_version: u16,
        ) -> Self {
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
        pub fn client_order_id(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn account_id(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 8)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn orig_client_order_id(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 12)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn client_ts(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 20)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn price(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 28)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn qty(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 36)
        }
    }
} // end decoder
