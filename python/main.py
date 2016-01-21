from startwinning import StartWinningAPI


def main():
    swapi = StartWinningAPI(2015)
    swapi.update_gamelogs()
    swapi.update_averages()
    swapi.update_rankings()

if __name__ == '__main__':
    main()
