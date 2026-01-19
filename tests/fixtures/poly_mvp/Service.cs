public class UserRepo
{
    public string GetUser(int id)
    {
        var sql = "select * from dbo.get_user(@id)";
        return sql;
    }
}
