# ArgentSea Orleans Library

This library is the abstract implementation of ArgentSea for Microsoft Orleans.

This is a core library is used by ArgentSea.Orleans.Sql. It is unlikely that you will need to reference this library directly.

## Limitations

Currently, the ArgentSea library is unable to handle types wrapped in `Orleans.Concurrency.Immutable<T>`. The core ArgentSea project builds the expression trees to set values, but does not know about Orleans objects; consequently, it cannot currently be taught how to handle this type. 

The `[Immutable]` attribute should still work.

## Contributions

Contributions are very welcome.

## License

[MIT.](https://opensource.org/licenses/MIT)


