/*******************************************************************************
 * thrill/vfs/s3_file.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/s3_file.hpp>

#include <thrill/api/context.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>

#if THRILL_USE_AWS

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#endif

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace vfs {

#if THRILL_USE_AWS

class S3File final : public virtual ReadStream, public virtual WriteStream
{
    static constexpr bool debug = false;

public:
    S3File() : is_valid_(false), is_read_(false) { }

    S3File(Aws::S3::Model::GetObjectResult&& gor, size_t range_start)
        : gor_(std::move(gor)), range_start_(range_start),
          is_valid_(true), is_read_(true) { }

    S3File(std::shared_ptr<Aws::S3::S3Client> client, const std::string& path)
        : client_(client), path_(path), is_valid_(true), is_read_(false) { }

    //! non-copyable: delete copy-constructor
    S3File(const S3File&) = delete;
    //! non-copyable: delete assignment operator
    S3File& operator = (const S3File&) = delete;

    //! move-constructor
    S3File(S3File&& f) noexcept
        : gor_(std::move(f.gor_)), write_stream_(std::move(f.write_stream_)),
          client_((f.client_)), path_(f.path_), is_valid_(f.is_valid_),
          is_read_(f.is_read_) {
        assert(0);
        f.is_valid_ = false;
    }

    ssize_t write(const void* data, size_t count) final {
        assert(is_valid_);
        assert(!is_read_);
        ssize_t before = write_stream_.tellp();
        write_stream_.write((const char*)data, count);
        return write_stream_.tellp() - before;
    }

    //! POSIX read function.
    ssize_t read(void* data, size_t count) final {
        LOG1 << "readcount " << count;
        assert(is_valid_);
        assert(is_read_);
        return gor_.GetBody().readsome((char*)data, count);
    }

    //! Emulates a seek. Due to HTTP Range requests we only load the file from
    //! first byte in local range. Therefore we don't need to actually seek and
    //! only have to emulate it by returning the 'seeked' offset.
    ssize_t lseek(off_t offset) final {
        assert(is_valid_);
        assert(is_read_);
        assert(offset == (off_t)range_start_);
        return offset;
    }

    void close() final {
        if (!is_read_ && is_valid_) {
            LOG << "Closing write file, uploading";
            Aws::S3::Model::PutObjectRequest por;

            std::string path_without_s3 = path_.substr(5);
            std::vector<std::string> splitted = common::Split(
                path_without_s3, '/', (std::string::size_type)2);

            Aws::S3::Model::CreateBucketRequest createBucketRequest;
            createBucketRequest.SetBucket(splitted[0]);

            client_->CreateBucket(createBucketRequest);

            por.SetBucket(splitted[0]);
            por.SetKey(splitted[1]);
            std::shared_ptr<Aws::IOStream> stream = std::make_shared<Aws::IOStream>(
                write_stream_.rdbuf());
            por.SetBody(stream);
            por.SetContentLength(write_stream_.tellp());
            auto outcome = client_->PutObject(por);
            if (!outcome.IsSuccess()) {
                throw common::ErrnoException(
                          "Download from S3 Errored: " + outcome.GetError().GetMessage());
            }
        }

        is_valid_ = false;
    }

private:
    Aws::S3::Model::GetObjectResult gor_;

    Aws::StringStream write_stream_;
    std::shared_ptr<Aws::S3::S3Client> client_;
    std::string path_;

    size_t range_start_;

    bool is_valid_;
    bool is_read_;
};

/******************************************************************************/

ReadStreamPtr S3OpenReadStream(
    const std::string& path, const api::Context& ctx,
    const common::Range& my_range) {

    static constexpr bool debug = false;

    LOG1 << "Opening file";

    // Amount of additional bytes read after end of range
    size_t maximum_line_length = 64 * 1024;

    Aws::S3::Model::GetObjectRequest getObjectRequest;

    std::string path_without_s3 = path.substr(5);

    std::vector<std::string> splitted = common::Split(
        path_without_s3, '/', (std::string::size_type)2);

    assert(splitted.size() == 2);

    getObjectRequest.SetBucket(splitted[0]);
    getObjectRequest.SetKey(splitted[1]);

    LOG << "Attempting to read from bucket " << splitted[0] << " with key "
        << splitted[1] << "!";

    size_t range_start = 0;
#if THIS_IS_NONSENSE_DESIGN
    if (/* !compressed */ true) {
        std::string range = "bytes=";
        bool use_range_ = false;
        if (my_range.begin > file.size_ex_psum) {
            range += std::to_string(my_range.begin - file.size_ex_psum);
            range_start = my_range.begin - file.size_ex_psum;
            use_range_ = true;
        }
        else {
            range += "0";
        }

        range += "-";
        if (my_range.end + maximum_line_length < file.size_inc_psum()) {
            range += std::to_string(file.size - (file.size_inc_psum() -
                                                 my_range.end -
                                                 maximum_line_length));
            use_range_ = true;
        }

        if (use_range_)
            getObjectRequest.SetRange(range);
    }
#endif

    LOG1 << "Get...";
    auto outcome = ctx.s3_client()->GetObject(getObjectRequest);
    LOG1 << "...Got";

    if (!outcome.IsSuccess())
        throw common::ErrnoException(
                  "Download from S3 Errored: " + outcome.GetError().GetMessage());

    return common::MakeCounting<S3File>(outcome.GetResultWithOwnership(),
                                        range_start);
}

WriteStreamPtr S3OpenWriteStream(
    const std::string& path, const api::Context& ctx) {
    return common::MakeCounting<S3File>(ctx.s3_client(), path);
}

#else   // !THRILL_USE_AWS

ReadStreamPtr S3OpenReadStream(
    const std::string& path, const api::Context& ctx,
    const common::Range& my_range) {
    return nullptr;
}

WriteStreamPtr S3OpenWriteStream(
    const std::string& path, const api::Context& ctx) {
    return nullptr;
}

#endif

} // namespace vfs
} // namespace thrill

/******************************************************************************/
