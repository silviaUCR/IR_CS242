// Create the search filter
app.filter('searchFor', function(){
    // All filters must return a function. The first parameter
    // is the data that is to be filtered, and the second is an
    // argument that may be passed with a colon (searchFor:searchString)
    return function(dataArr, searchString){

        if(!searchString){
            return dataArr;
        }

        var result = [];
        searchString = searchString.toLowerCase();

        // Using the forEach helper method to loop through the array
        angular.forEach(dataArr, function(item){
            if(item.name.toLowerCase().indexOf(searchString) !== -1){
                result.push(item);
            }
        });
        return result;
    };
});

// The controller
function searchController($scope){
    // The data model. These items would normally be requested via AJAX,
    // but are hardcoded here for simplicity. See the next example for
    // tips on using AJAX.
    $scope.movies = [
        {name:"The Godfather",releaseYr:"1972"},
        {name:"Inception",releaseYr:"2010"},
        {name:"Titanic",releaseYr:"1997"},
        {name:"Thor:Ragnarok",releaseYr:"2017"},
        {name:"Forest Gump",releaseYr:"1994"},
        {name:"Star Wars",releaseYr:"1977"},
        {name:"Justice League",releaseYr:"2017"},
        {name:"The Dark Knight",releaseYr:"2008"},
        {name:"Avatar",releaseYr:"2009"},
        {name:"The Avengers",releaseYr:"2012"}
    ];}