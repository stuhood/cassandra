package org.apache.cassandra.io.sstable;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.File;
import java.util.Map;
import java.util.StringTokenizer;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.sstable.Component.separator;

/**
 * A SSTable is described by the keyspace and column family it contains data
 * for, a generation (where higher generations contain more recent data) and
 * an alphabetic version string.
 *
 * A descriptor can be marked as temporary, which influences generated filenames.
 */
public class Descriptor
{
    public static final String LEGACY_VERSION = "a"; // "pre-history"
    public static final String CURRENT_VERSION = "ha";
    public static final Map<String,Version> VERSIONS;
    static
    {
        ImmutableMap.Builder builder = ImmutableMap.builder();
        // b (0.7.0): added version to sstable filenames
        builder.put("a", new Version("a", "01111110"));
        builder.put("b", new Version("b", "01111110"));
        // c (0.7.0): bloom filter component computes hashes over raw key bytes instead of strings
        builder.put("c", new Version("c", "00111110"));
        // d (0.7.0): row size in data component becomes a long instead of int
        builder.put("d", new Version("d", "00011110"));
        // e (0.7.0): stores undecorated keys in data and index components
        builder.put("e", new Version("e", "00001110"));
        // f (0.7.0): switched bloom filter implementations in data component
        builder.put("f", new Version("f", "00000110"));
        // g (0.8): tracks flushed-at context in metadata component
        builder.put("g", new Version("g", "00000011"));
        // h (1.0): tracks max client timestamp in metadata component
        builder.put("h", new Version("h", "00000011"));
        // ha (1.0+): type specific data and index file compression
        builder.put("ha", new Version("ha", "00000001"));
        VERSIONS = builder.build();
    };

    public final File directory;
    public final Version version;
    public final String ksname;
    public final String cfname;
    public final int generation;
    public final boolean temporary;
    private final int hashCode;

    public static final class Version
    {
        public final String version;
        public final boolean isLatestVersion;                           // 0
        public final boolean hasStringsInBloomFilter;                   // 1
        public final boolean hasIntRowSize;                             // 2
        public final boolean hasEncodedKeys;                            // 3
        public final boolean usesOldBloomFilter;                        // 4
        public final boolean usesHistogramAndReplayPositionStatsFile;   // 5
        public final boolean isRowIndexed;                              // 6
        public final boolean hasReplayPosition;                         // 7
        /** Takes a string representing all possible features as binary: 0/1. */
        private Version(String version, String features)
        {
            assert features.length() == 8;
            this.version = version;
            this.isLatestVersion = features.charAt(0) == '1';
            this.hasStringsInBloomFilter = features.charAt(1) == '1';
            this.hasIntRowSize = features.charAt(2) == '1';
            this.hasEncodedKeys = features.charAt(3) == '1';
            this.usesOldBloomFilter = features.charAt(4) == '1';
            this.usesHistogramAndReplayPositionStatsFile = features.charAt(5) == '1';
            this.isRowIndexed = features.charAt(6) == '1';
            this.hasReplayPosition = features.charAt(7) == '1';
        }
    }

    public static final class UnknownVersionException extends RuntimeException
    {
        private UnknownVersionException(String msg)
        {
            super(msg);
        }
    }

    public enum TempState
    {
        LIVE,
        TEMP,
        ANY;

        boolean isMatch(Descriptor descriptor)
        {
            assert descriptor != null;
            if (TempState.ANY == this)
                return true;
            return (TempState.TEMP == this) ? descriptor.temporary : !descriptor.temporary;
        }
    }

    public Descriptor(String version, File directory, String ksname, String cfname, int generation, boolean temp) throws UnknownVersionException
    {
        assert version != null && directory != null && ksname != null && cfname != null;
        if (!VERSIONS.containsKey(version))
            throw new UnknownVersionException("Unreadable version '" + version + "' (known versions: " + VERSIONS.keySet() + ") for file in " + directory);
        this.version = VERSIONS.get(version);
        this.directory = directory;
        this.ksname = ksname;
        this.cfname = cfname;
        this.generation = generation;
        temporary = temp;
        hashCode = Objects.hashCode(directory, generation, ksname, cfname);
    }

    public String filenameFor(Component component)
    {
        return filenameFor(component.name());
    }
    
    private String baseFilename()
    {
        StringBuilder buff = new StringBuilder();
        buff.append(directory).append(File.separatorChar);
        buff.append(cfname).append(separator);
        if (temporary)
            buff.append(SSTable.TEMPFILE_MARKER).append(separator);
        if (!LEGACY_VERSION.equals(version.version))
            buff.append(version.version).append(separator);
        buff.append(generation);
        return buff.toString();
    }

    /**
     * @param suffix A component suffix, such as 'Data.db'/'Index.db'/etc
     * @return A filename for this descriptor with the given suffix.
     */
    public String filenameFor(String suffix)
    {
        return baseFilename() + separator + suffix;
    }

    /**
     * @see #fromFilename(File directory, String name)
     * @param filename The SSTable filename
     * @return Descriptor of the SSTable initialized from filename
     */
    public static Descriptor fromFilename(String filename)
    {
        File file = new File(filename);
        assert file.getParentFile() != null : "Filename must include parent directory.";
        return fromFilename(file.getParentFile(), file.getName()).left;
    }

    /**
     * Filename of the form "<ksname>/<cfname>-[tmp-][<version>-]<gen>-<component>"
     *
     * @param directory The directory of the SSTable files
     * @param name The name of the SSTable file
     *
     * @return A Descriptor for the SSTable, and the Component remainder.
     */
    public static Pair<Descriptor,String> fromFilename(File directory, String name)
    {
        // name of parent directory is keyspace name
        String ksname = extractKeyspaceName(directory);

        // tokenize the filename
        StringTokenizer st = new StringTokenizer(name, String.valueOf(separator));
        String nexttok;

        // all filenames must start with a column family
        String cfname = st.nextToken();

        // optional temporary marker
        nexttok = st.nextToken();
        boolean temporary = false;
        if (nexttok.equals(SSTable.TEMPFILE_MARKER))
        {
            temporary = true;
            nexttok = st.nextToken();
        }

        // optional version string
        String version = LEGACY_VERSION;
        if (versionValidate(nexttok))
        {
            version = nexttok;
            nexttok = st.nextToken();
        }
        int generation = Integer.parseInt(nexttok);

        // component suffix
        String component = st.nextToken();

        return new Pair<Descriptor,String>(new Descriptor(version, directory, ksname, cfname, generation, temporary), component);
    }

    /**
     * Extracts the keyspace name out of the directory name. Snapshot directories have a slightly different
     * path structure and need to be treated differently.
     *
     * Regular path:   "<ksname>/<cfname>-[tmp-][<version>-]<gen>-<component>"
     * Snapshot path: "<ksname>/snapshots/<snapshot-name>/<cfname>-[tmp-][<version>-]<gen>-<component>"
     *
     * @param directory a directory containing SSTables
     * @return the keyspace name
     */
    public static String extractKeyspaceName(File directory)
    {
        if (isSnapshotInPath(directory))
        {
            // We need to move backwards. If this is a snapshot, first parent takes us to:
            // <ksname>/snapshots/ and second call to parent takes us to <ksname>.
            return directory.getParentFile().getParentFile().getName();
        }
        return directory.getName();
    }

    /**
     * @param directory The directory to check
     * @return <code>TRUE</code> if this directory represents a snapshot directory. <code>FALSE</code> otherwise.
     */
    private static boolean isSnapshotInPath(File directory)
    {
        File curDirectory = directory;
        while (curDirectory != null)
        {
            if (curDirectory.getName().equals(Table.SNAPSHOT_SUBDIR_NAME))
                return true;
            curDirectory = curDirectory.getParentFile();
        }

        // The directory does not represent a snapshot directory.
        return false;
    }

    /**
     * @param temporary temporary flag
     * @return A clone of this descriptor with the given 'temporary' status.
     */
    public Descriptor asTemporary(boolean temporary)
    {
        return new Descriptor(version.version, directory, ksname, cfname, generation, temporary);
    }

    /**
     * @param ver SSTable version
     * @return True if the given version string is not empty, and
     * contains all lowercase letters, as defined by java.lang.Character.
     */
    public static boolean versionValidate(String ver)
    {
        if (ver.length() < 1) return false;
        for (char ch : ver.toCharArray())
            if (!Character.isLetter(ch) || !Character.isLowerCase(ch))
                return false;
        return true;
    }

    @Override
    public String toString()
    {
        return baseFilename();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof Descriptor))
            return false;
        Descriptor that = (Descriptor)o;
        return that.directory.equals(this.directory) && that.generation == this.generation && that.ksname.equals(this.ksname) && that.cfname.equals(this.cfname);
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
