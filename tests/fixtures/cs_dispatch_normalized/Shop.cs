using System;
using System.Collections.Generic;

namespace Shop
{
    public interface IA<T>
    {
        void Run();
    }

    public class C : IA<int>, IA<List<int>>, IA<string>
    {
        void IA<Int32>.Run()
        {
        }

        void IA<List<Int32>>.Run()
        {
        }

        void IA<String?>.Run()
        {
        }
    }

    public class CInt
    {
        public void Go(IA<int> a)
        {
            a.Run();
        }
    }

    public class CList
    {
        public void Go(IA< List< int > > a)
        {
            a.Run();
        }
    }

    public class CSystemString
    {
        public void Go(IA<System.String> a)
        {
            a.Run();
        }
    }

    public class CNullableString
    {
        public void Go(IA<string?> a)
        {
            a.Run();
        }
    }
}
