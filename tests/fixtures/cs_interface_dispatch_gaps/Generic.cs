namespace Shop
{
    public interface IRepo<T>
    {
        void Save(T item);
    }

    public class Order
    {
    }

    public class Repo : IRepo<Order>
    {
        public void Save(Order item)
        {
        }
    }

    public class RepoCaller
    {
        private readonly IRepo<Order> _repo;

        public RepoCaller(IRepo<Order> repo)
        {
            _repo = repo;
        }

        public void Store(Order o)
        {
            _repo.Save(o);
        }
    }
}
