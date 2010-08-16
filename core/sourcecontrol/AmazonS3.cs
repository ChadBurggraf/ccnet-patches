using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Exortech.NetReflector;
using ThoughtWorks.CruiseControl.Core.Util;

namespace ThoughtWorks.CruiseControl.Core.Sourcecontrol
{
    /// <summary>
    /// <para>
    /// Use the &quot;amazonS3&quot; source control block to check for modifications in an Amazon S3 bucket.
    /// Files are compared based on size and last modification date. All modification types are detected,
    /// including create, update and delete.
    /// </para>
    /// <para type="warning">
    /// You will get a trust relationship SSL error if you set useSsl to true and you have periods in
    /// your bucket name. You can read the discussion here: http://developer.amazonwebservices.com/connect/click.jspa?searchID=-1&messageID=155837
    /// for more information (the wildcard mapping on the SSL certificate will only go up one level,
    /// so your entire bucket name must map to a single subdomain level).
    /// </para>
    /// <title>Amazon S3 Source Control Block</title>
    /// <version>1.0</version>
    /// <key name="type">
    /// <description>The type of source control block.</description>
    /// <value>amazonS3</value>
    /// </key>
    /// <code title="Minimalist Example">
    /// &lt;sourcecontrol type="amazonS3"&gt;
    /// &lt;acessKeyId&gt;Your_AWS_Access_Key&lt;/repository&gt;
    /// &lt;secretAccessKeyId&gt;Your_AWS_Secret_Access_Key&lt;/secretAccessKey&gt;
    /// &lt;bucket&gt;my.bucket&lt;/bucket&gt;
    /// &lt;/sourcecontrol&gt;
    /// </code>
    /// <code title="Full Example">
    /// &lt;sourcecontrol type="amazonS3"&gt;
    /// &lt;acessKeyId&gt;Your_AWS_Access_Key&lt;/repository&gt;
    /// &lt;secretAccessKeyId&gt;Your_AWS_Secret_Access_Key&lt;/secretAccessKey&gt;
    /// &lt;bucket&gt;my.bucket&lt;/bucket&gt;
    /// &lt;autoGetSource&gt;true&lt;/autoGetSource&gt;
    /// &lt;ignoreMissingRoot&gt;false&lt;/ignoreMissingRoot&gt;
    /// &lt;prefix&gt;some/path/here/&lt;/prefix&gt;
    /// &lt;useSsl&gt;true&lt;/useSsl&gt;
    /// &lt;/sourcecontrol&gt;
    /// </code>
    /// </summary>
    [ReflectorType("amazonS3")]
    public class AmazonS3
        : SourceControlBase
    {
        #region Private Fields

        private Modification[] modifications;

        #endregion

        #region Public Instance Properties

        /// <summary>
        /// The Amazon access key ID to use when connecting to the S3 service.
        /// </summary>
        /// <version>1.5</version>
        /// <default>n/a</default>
        [ReflectorProperty("accessKeyId", Required = true)]
        public string AccessKeyId { get; set; }

        /// <summary>
        /// A value indicating whether to automatically (recursively) copy the contents
        /// of the Amazon S3 bucket (restricted by the configured prefix, if applicable) to the project
        /// working directory.
        /// </summary>
        /// <version>1.5</version>
        /// <default>false</default>
        [ReflectorProperty("autoGetSource", Required = false)]
        public bool AutoGetSource { get; set; }

        /// <summary>
        /// The Amazon S3 bucket to connect to.
        /// </summary>
        /// <version>1.5</version>
        /// <default>n/a</default>
        [ReflectorProperty("bucket", Required = true)]
        public string Bucket { get; set; }

        /// <summary>
        /// A value indicating whether to prevent build failure if the configured bucket and prefix (if applicable)
        /// are not found.
        /// </summary>
        /// <version>1.5</version>
        /// <default>false</default>
        [ReflectorProperty("ignoreMissingRoot", Required = false)]
        public bool IgnoreMissingRoot { get; set; }

        /// <summary>
        /// The prefix to restrict the repository definition to within the configure Amazon S3 bucket.
        /// </summary>
        /// <version>1.5</version>
        /// <default>n/a</default>
        [ReflectorProperty("prefix", Required = false)]
        public string Prefix { get; set; }

        /// <summary>
        /// Gets or sets the Amazon secret access key ID to use when connecting to the S3 service.
        /// </summary>
        /// <version>1.5</version>
        /// <default>n/a</default>
        [ReflectorProperty("secretAccessKeyId", Required = true)]
        public string SecretAccesstKeyId { get; set; }

        /// <summary>
        /// A value indicating whether to connect to the Amazon S3 service using SSL.
        /// </summary>
        /// <version>1.5</version>
        /// <default>false</default>
        [ReflectorProperty("useSsl", Required = false)]
        public bool UseSsl { get; set; }

        #endregion

        #region SourceControlBase Methods

        public override Modification[] GetModifications(IIntegrationResult fromResult, IIntegrationResult toResult)
        {
            this.modifications = this.GetModifications(this.CreateClient(), fromResult.WorkingDirectory).ToArray();
            return this.modifications;
        }

        public override void LabelSourceControl(IIntegrationResult result)
        {
        }

        public override void GetSource(IIntegrationResult result)
        {
            result.BuildProgressInformation.SignalStartRunTask("Getting source from Amazon S3");

            if (this.AutoGetSource)
            {
                Amazon.S3.AmazonS3 client = this.CreateClient();

                if (this.modifications == null)
                {
                    this.modifications = this.GetModifications(client, result.WorkingDirectory).ToArray();
                }

                foreach (var modification in this.modifications)
                {
                    string path = Path.Combine(modification.FolderName, modification.FileName);

                    if (modification.Type == "Deleted" && File.Exists(path))
                    {
                        File.Delete(path);

                        if (!result.WorkingDirectory.Equals(modification.FolderName, StringComparison.OrdinalIgnoreCase) && 
                            Directory.GetFileSystemEntries(modification.FolderName).Length == 0)
                        {
                            Directory.Delete(modification.FolderName);
                        }
                    }
                    else
                    {
                        this.GetSource(client, modification);
                    }
                }
            }
        }

        public override void Initialize(IProject project)
        {   
        }

        public override void Purge(IProject project)
        {
        }

        #endregion

        #region Private Instance Methods

        /// <summary>
        /// Creates a new Amazon S3 client using this instance's values for initialization and configuration.
        /// </summary>
        /// <returns>The created Amazon S3 client.</returns>
        private Amazon.S3.AmazonS3 CreateClient()
        {
            AmazonS3Config config = new AmazonS3Config()
            {
                CommunicationProtocol = this.UseSsl ? Protocol.HTTPS : Protocol.HTTP
            };

            return AWSClientFactory.CreateAmazonS3Client(this.AccessKeyId, this.SecretAccesstKeyId, config);
        }

        /// <summary>
        /// Creates a <see cref="Modification"/> instance for the given Amazon S3 object.
        /// </summary>
        /// <param name="workingDirectory">The current project's working directory.</param>
        /// <param name="fileObject">The <see cref="FileObject"/> to get a modification for.</param>
        /// <param name="type">The modification type.</param>
        /// <returns>The created <see cref="Modification"/>.</returns>
        private Modification CreateModification(string workingDirectory, FileObject fileObject, string type)
        {
            string path = fileObject.Key;

            if (!String.IsNullOrEmpty(this.Prefix))
            {
                path = path.Substring(this.Prefix.Length);
            }

            if (path.StartsWith("/", StringComparison.Ordinal))
            {
                path = path.Substring(1);
            }

            path = Path.Combine(workingDirectory, path.Replace('/', Path.DirectorySeparatorChar));

            return new Modification()
            {
                FileName = Path.GetFileName(path),
                FolderName = Path.GetDirectoryName(path),
                ModifiedTime = fileObject.LastModified.ToLocalTime(),
                Type = type,
                Url = fileObject.Key
            };
        }

        /// <summary>
        /// Gets a collection of <see cref="FileObject"/> instances that correspond to local filesystem entries.
        /// </summary>
        /// <param name="workingDirectory">The current project's working directory.</param>
        /// <returns>The local filesystem as a collection of <see cref="FileObject"/>.</returns>
        private List<FileObject> GetLocalContents(string workingDirectory)
        {
            List<FileObject> local = new List<FileObject>();
            this.GetLocalContents(workingDirectory, workingDirectory, local);

            return (from o in local
                    orderby o.Key
                    select o).ToList();
        }

        /// <summary>
        /// Recursive helper for GetLocalContents().
        /// </summary>
        /// <param name="workingDirectory">The current project's working directory.</param>
        /// <param name="directory">The current directory being recursed.</param>
        /// <param name="local">The list of objects to add filesystem entries to.</param>
        private void GetLocalContents(string workingDirectory, string directory, List<FileObject> local)
        {
            foreach (string path in Directory.GetDirectories(directory))
            {
                this.GetLocalContents(workingDirectory, path, local);
            }

            foreach (string path in Directory.GetFiles(directory))
            {
                string relativePath = path.Substring(workingDirectory.Length).Replace(Path.DirectorySeparatorChar, '/');

                if (relativePath.StartsWith("/", StringComparison.Ordinal))
                {
                    relativePath = relativePath.Substring(1);
                }

                string prefix = this.Prefix ?? String.Empty;

                if (!String.IsNullOrEmpty(prefix) && !prefix.EndsWith("/"))
                {
                    prefix += "/";
                }

                local.Add(new FileObject(prefix + relativePath, new FileInfo(path)));
            }
        }

        /// <summary>
        /// Gets the modifications that represent the delta between the local contents and the repository contents.
        /// </summary>
        /// <param name="client">The Amazon S3 client to use when connecting to the service.</param>
        /// <param name="workingDirectory">The current project's working directory.</param>
        /// <returns>A collection of modifications.</returns>
        private IEnumerable<Modification> GetModifications(Amazon.S3.AmazonS3 client, string workingDirectory)
        {
            var repositoryContents = this.GetRepositoryContents(client);
            var localContents = this.GetLocalContents(workingDirectory);

            var created = from r in repositoryContents
                          let l = (from l in localContents where l.Key == r.Key select l).FirstOrDefault()
                          where l == null
                          select this.CreateModification(workingDirectory, r, "Created");

            var updated = from r in repositoryContents
                          let l = (from l in localContents where l.Key == r.Key select l).FirstOrDefault()
                          where l != null && (r.LastModifiedGMTString != l.LastModifiedGMTString || r.Size != l.Size)
                          select this.CreateModification(workingDirectory, r, "Updated");

            var deleted = from l in localContents
                          let r = (from r in repositoryContents where r.Key == l.Key select r).FirstOrDefault()
                          where r == null
                          select this.CreateModification(workingDirectory, l, "Deleted");

            return created.Concat(updated).Concat(deleted).ToArray();
        }

        /// <summary>
        /// Gets the metadata contents of the repository on Amazon S3.
        /// </summary>
        /// <param name="client">The Amazon S3 client to use when connecting to the service.</param>
        /// <returns>The current metadata contents of the repository.</returns>
        private IEnumerable<FileObject> GetRepositoryContents(Amazon.S3.AmazonS3 client)
        {
            List<FileObject> objects = new List<FileObject>();
            string marker = String.Empty;
            bool truncated = true;

            while (truncated)
            {
                ListObjectsRequest request = new ListObjectsRequest()
                    .WithBucketName(this.Bucket)
                    .WithPrefix(this.Prefix ?? String.Empty)
                    .WithMarker(marker);

                try
                {
                    using (ListObjectsResponse response = client.ListObjects(request))
                    {
                        objects.AddRange(response.S3Objects.Select(o => new FileObject(o)));

                        if (response.IsTruncated)
                        {
                            marker = objects[objects.Count - 1].Key;
                        }
                        else
                        {
                            truncated = false;
                        }
                    }
                }
                catch (AmazonS3Exception)
                {
                    if (this.IgnoreMissingRoot)
                    {
                        truncated = false;
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            return objects;
        }

        /// <summary>
        /// Gets the source for the given modification and saves it to the modification's path.
        /// Assumes the Amazon S3 object key is stored in the modification's Url property.
        /// </summary>
        /// <param name="client">The Amazon S3 client to use when connecting to the service.</param>
        /// <param name="modification">The <see cref="Modification"/> identifying the object and where to save it.</param>
        private void GetSource(Amazon.S3.AmazonS3 client, Modification modification)
        {
            if (!String.IsNullOrEmpty(modification.Url))
            {
                GetObjectRequest request = new GetObjectRequest()
                    .WithBucketName(this.Bucket)
                    .WithKey(modification.Url);

                string path = Path.Combine(modification.FolderName, modification.FileName);

                if (!Directory.Exists(modification.FolderName))
                {
                    Directory.CreateDirectory(modification.FolderName);
                }

                using (FileStream file = File.Create(path))
                {
                    using (GetObjectResponse response = client.GetObject(request))
                    {
                        byte[] buffer = new byte[4096];
                        int count = 0;

                        while (0 < (count = response.ResponseStream.Read(buffer, 0, buffer.Length)))
                        {
                            file.Write(buffer, 0, count);
                        }
                    }
                }

                File.SetLastWriteTime(path, modification.ModifiedTime);
            }
        }

        #endregion

        #region Private FileObject Helper Class

        /// <summary>
        /// Represents a generic view of an Amazon S3 object and a local filesystem entry.
        /// </summary>
        private class FileObject
        {
            /// <summary>
            /// Initializes a new instance of the FileObject class.
            /// </summary>
            /// <param name="s3Object">The Amazon S3 object to create this instance from.</param>
            public FileObject(S3Object s3Object)
            {
                this.Key = s3Object.Key;
                this.LastModified = DateTime.Parse(s3Object.LastModified, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).ToUniversalTime();
                this.LastModifiedGMTString = String.Format(CultureInfo.InvariantCulture, "{0:s}Z", this.LastModified);
                this.Size = Int64.Parse(s3Object.Size, CultureInfo.InvariantCulture);
            }

            /// <summary>
            /// Initializes a new instance of the FileObject class.
            /// </summary>
            /// <param name="key">The equivalent Amazon S3 object key this file represents.</param>
            /// <param name="info">The filesystem object to create this instance from.</param>
            public FileObject(string key, FileInfo info)
            {
                this.Key = key;
                this.LastModified = info.LastWriteTimeUtc;
                this.LastModifiedGMTString = String.Format(CultureInfo.InvariantCulture, "{0:s}Z", this.LastModified);
                this.Size = info.Length;
            }

            /// <summary>
            /// Gets the object's Amazon S3 key.
            /// </summary>
            public string Key { get; private set; }

            /// <summary>
            /// Gets the object's last modified date in UTC.
            /// </summary>
            public DateTime LastModified { get; private set; }

            /// <summary>
            /// Gets the object's last modified date as a sortable GMT string.
            /// </summary>
            public string LastModifiedGMTString { get; private set; }

            /// <summary>
            /// Gets the object's size in bytes.
            /// </summary>
            public long Size { get; private set; }
        }

        #endregion
    }
}
