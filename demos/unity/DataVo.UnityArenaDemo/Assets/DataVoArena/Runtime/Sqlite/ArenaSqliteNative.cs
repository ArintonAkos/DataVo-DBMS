#nullable enable

using System;
using System.Runtime.InteropServices;

namespace DataVo.UnityArena.Sqlite
{
    public static class ArenaSqliteNative
    {
        public const int SQLITE_OK = 0;
        public const int SQLITE_ROW = 100;
        public const int SQLITE_DONE = 101;
        public const int SQLITE_OPEN_READWRITE = 0x00000002;
        public const int SQLITE_OPEN_CREATE = 0x00000004;
        public const int SQLITE_OPEN_MEMORY = 0x00000080;

        internal static readonly IntPtr SQLITE_TRANSIENT = new(-1);

        private const string LibraryName = "sqlite3";
        private const string UnavailableMessage =
            "macOS sqlite3 native library is unavailable; Unity cross-engine SQLite benchmark cannot run.";

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern int sqlite3_open_v2(
            [MarshalAs(UnmanagedType.LPUTF8Str)] string filename,
            out IntPtr database,
            int flags,
            IntPtr virtualFileSystem);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern int sqlite3_close_v2(IntPtr database);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern int sqlite3_exec(
            IntPtr database,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string sql,
            IntPtr callback,
            IntPtr callbackArgument,
            IntPtr errorMessage);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern int sqlite3_prepare_v2(
            IntPtr database,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string sql,
            int byteCount,
            out IntPtr statement,
            IntPtr tail);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern int sqlite3_bind_int(IntPtr statement, int index, int value);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern int sqlite3_bind_text(
            IntPtr statement,
            int index,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string value,
            int byteCount,
            IntPtr destructor);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern int sqlite3_step(IntPtr statement);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern int sqlite3_column_int(IntPtr statement, int columnIndex);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern IntPtr sqlite3_column_text(IntPtr statement, int columnIndex);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern int sqlite3_finalize(IntPtr statement);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern IntPtr sqlite3_errmsg(IntPtr database);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern IntPtr sqlite3_libversion();

        public static string GetLibraryVersion() =>
            RequireAvailableForTest(() => Marshal.PtrToStringUTF8(sqlite3_libversion()) ?? string.Empty);

        public static string RequireAvailableForTest(Func<string> getVersion)
        {
            if (getVersion is null)
            {
                throw new ArgumentNullException(nameof(getVersion));
            }

            try
            {
                string version = getVersion();
                if (string.IsNullOrWhiteSpace(version))
                {
                    throw new InvalidOperationException(UnavailableMessage);
                }

                return version;
            }
            catch (DllNotFoundException)
            {
                throw new InvalidOperationException(UnavailableMessage);
            }
            catch (EntryPointNotFoundException)
            {
                throw new InvalidOperationException(UnavailableMessage);
            }
        }

        internal static void RequireStatus(IntPtr database, int status, string operation, bool acceptsRow = false, bool acceptsDone = false)
        {
            if (status == SQLITE_OK || (acceptsRow && status == SQLITE_ROW) || (acceptsDone && status == SQLITE_DONE))
            {
                return;
            }

            string error = database == IntPtr.Zero
                ? "SQLite did not provide an error message."
                : Marshal.PtrToStringUTF8(sqlite3_errmsg(database)) ?? "SQLite did not provide an error message.";
            throw new InvalidOperationException(
                operation + " failed with SQLite status " + status.ToString(System.Globalization.CultureInfo.InvariantCulture) + ": " + error);
        }
    }
}
